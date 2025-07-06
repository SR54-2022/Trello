package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"io"
	"log"
	"os"
	"time"

	"analytics-service/model"
	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/gofrs/uuid"
)

var (
	cacheProjectConstruct = "project:%s"
)

func constructKeyForProject(projectID string) string {
	return fmt.Sprintf(cacheProjectConstruct, projectID)
}

type ESDBClient struct {
	client *esdb.Client
	group  string
	sub    *esdb.PersistentSubscription
	rds    *redis.Client
	tracer trace.Tracer
}

func NewESDBClient(client *esdb.Client, group string, tracer trace.Tracer) (*ESDBClient, error) {
	opts := esdb.PersistentAllSubscriptionOptions{
		From: esdb.Start{},
	}

	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")
	redisAddress := fmt.Sprintf("%s:%s", redisHost, redisPort)

	cl := redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})

	if err := cl.Ping().Err(); err != nil {
		return nil, err
	}

	// Attempt to create the subscription
	err := client.CreatePersistentSubscriptionAll(context.Background(), group, opts)
	if err != nil {
		// persistent subscription group already exists
		log.Println(err)
	}

	esdbClient := &ESDBClient{
		client: client,
		group:  group,
		rds:    cl,
		tracer: tracer,
	}

	// Ensure the subscription is set up
	if err := esdbClient.subscribe(); err != nil {
		log.Println("Subscription error:", err)
		return nil, err
	}
	return esdbClient, nil
}

func (e *ESDBClient) StoreEvent(ctx context.Context, stream string, event model.Event) error {
	ctx, span := e.tracer.Start(ctx, "EventRepository.StoreEvent")
	defer span.End()

	id, err := uuid.NewV4()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	eventData, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	log.Printf("Storing event to stream: %s, event: %+v\n", stream, event)

	esEvent := esdb.EventData{
		EventID:     id,
		EventType:   string(event.Type),
		Data:        eventData,
		ContentType: esdb.JsonContentType,
	}
	redisKey := constructKeyForProject(stream)
	jsonData, err := e.rds.Get(redisKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	var events []model.Event
	if jsonData != "" {
		err = json.Unmarshal([]byte(jsonData), &events)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
	}

	events = append(events, event)

	updatedData, err := json.Marshal(events)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	err = e.rds.Set(redisKey, string(updatedData), 3*time.Minute).Err()

	opts := esdb.AppendToStreamOptions{}
	_, err = e.client.AppendToStream(context.Background(), stream, opts, esEvent)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetStatus(codes.Ok, "")
	return nil
}

func (e *ESDBClient) ProcessEvents(processFn func(event model.Event) error) {
	for {
		receivedEvent := e.sub.Recv()

		if receivedEvent.EventAppeared != nil {
			streamEvent := receivedEvent.EventAppeared.Event
			var event model.Event
			if err := json.Unmarshal(streamEvent.Data, &event); err != nil {
				log.Println("Failed to deserialize event:", err)
				e.sub.Nack(err.Error(), esdb.Nack_Park, receivedEvent.EventAppeared)
				continue
			}
			err := processFn(event)
			if err != nil {
				log.Println("Processing error:", err)
				e.sub.Nack(err.Error(), esdb.Nack_Retry, receivedEvent.EventAppeared)
			} else {
				e.sub.Ack(receivedEvent.EventAppeared)
			}
		}

		if receivedEvent.SubscriptionDropped != nil {
			log.Println("Subscription dropped:", receivedEvent.SubscriptionDropped.Error)
			for err := e.subscribe(); err != nil; {
				log.Println("Reattempting subscription in 5 seconds...")
				time.Sleep(5 * time.Second)
			}
		}
	}
}

func (e *ESDBClient) subscribe() error {
	opts := esdb.ConnectToPersistentSubscriptionOptions{}
	sub, err := e.client.ConnectToPersistentSubscriptionToAll(context.Background(), e.group, opts)
	if err != nil {
		return err
	}
	e.sub = sub
	return nil
}

func (repo *ESDBClient) GetEventsByProjectID(ctx context.Context, projectID string) ([]model.Event, error) {
	ctx, span := repo.tracer.Start(ctx, "EventRepository.GetEventsByProjectID")
	defer span.End()

	streamName := fmt.Sprintf(projectID)
	redisKey := constructKeyForProject(projectID)

	// Try to fetch events from Redis
	events, err := repo.fetchEventsFromRedis(ctx, redisKey, span)
	if err != nil {
		return nil, err
	}

	if len(events) > 0 {
		log.Println("Returning events from Redis")
		span.SetStatus(codes.Ok, "")
		return events, nil
	}

	// Fetch events from EventStoreDB
	events, err = repo.fetchEventsFromEventStoreDB(ctx, streamName, span)
	if err != nil {
		return nil, err
	}

	// Cache events in Redis
	if err := repo.cacheEventsInRedis(redisKey, events, span); err != nil {
		return nil, err
	}

	log.Println("Returning events from EventStoreDB")
	span.SetStatus(codes.Ok, "")
	return events, nil
}

func (repo *ESDBClient) fetchEventsFromRedis(ctx context.Context, redisKey string, span trace.Span) ([]model.Event, error) {
	// Check if events exist in Redis
	exists, err := repo.rds.Exists(redisKey).Result()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Printf("Error checking Redis key existence: %v", err)
		return nil, fmt.Errorf("failed to check Redis key existence: %w", err)
	}

	if exists == 1 {
		// Fetch events from Redis
		jsonData, err := repo.rds.Get(redisKey).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			log.Printf("Error fetching data from Redis: %v", err)
			return nil, fmt.Errorf("failed to get data from Redis: %w", err)
		}

		if jsonData != "" {
			var events []model.Event
			if err := json.Unmarshal([]byte(jsonData), &events); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				log.Printf("Error unmarshalling Redis data: %v", err)
				return nil, fmt.Errorf("failed to unmarshal Redis data: %w", err)
			}
			log.Printf("Fetched from Redis: %d events\n", len(events))
			return events, nil
		}
	}

	return nil, nil
}

func (repo *ESDBClient) fetchEventsFromEventStoreDB(ctx context.Context, streamName string, span trace.Span) ([]model.Event, error) {
	var events []model.Event
	readOpts := esdb.ReadStreamOptions{From: esdb.Start{}}
	count := uint64(100)

	stream, err := repo.client.ReadStream(ctx, streamName, readOpts, count)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Printf("Error opening stream: %v", err)
		return nil, fmt.Errorf("failed to read stream: %w", err)
	}
	defer stream.Close()

	for {
		event, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			log.Printf("Error receiving event: %v", err)
			return nil, fmt.Errorf("failed to receive event: %w", err)
		}

		var e model.Event
		if err := json.Unmarshal(event.Event.Data, &e); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			log.Printf("Error unmarshalling event data: %v", err)
			continue
		}
		events = append(events, e)
	}

	log.Printf("Fetched from EventStoreDB: %d events\n", len(events))
	return events, nil
}

func (repo *ESDBClient) cacheEventsInRedis(redisKey string, events []model.Event, span trace.Span) error {
	if len(events) > 0 {
		jsonData, err := json.Marshal(events)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			log.Printf("Error marshalling events for Redis: %v", err)
			return fmt.Errorf("failed to marshal events for Redis: %w", err)
		}

		err = repo.rds.Set(redisKey, jsonData, 3*time.Minute).Err()
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			log.Printf("Error saving events to Redis: %v", err)
			return fmt.Errorf("failed to save events to Redis: %w", err)
		}
		log.Println("Cached events in Redis")
	}

	return nil
}
