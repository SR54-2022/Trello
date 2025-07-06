package repository

import (
	"context"
	"fmt"
	"github.com/gocql/gocql"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"log"
	"notification-service/model"
	"os"
	"strconv"
	"time"
)

type NotificationRepo struct {
	session *gocql.Session
	logger  *log.Logger
	tracer  trace.Tracer
}

const (
	successfulFunc = "Successful function!"
)

func New(logger *log.Logger, tracer trace.Tracer) (*NotificationRepo, error) {
	dbHost := os.Getenv("CASSANDRA_HOST")
	if dbHost == "" {
		logger.Println("Cassandra host is not set")
		return nil, fmt.Errorf("Cassandra host is not set")
	}

	dbPort := os.Getenv("CASSANDRA_PORT")
	if dbPort == "" {
		dbPort = "9042"
	}

	port, err := strconv.Atoi(dbPort)
	if err != nil {
		logger.Println("Invalid Cassandra port:", dbPort)
		return nil, fmt.Errorf("Invalid Cassandra port: %s", dbPort)
	}

	cluster := gocql.NewCluster(dbHost)
	cluster.Port = port
	cluster.Keyspace = "system"
	session, err := cluster.CreateSession()
	if err != nil {
		logger.Println("Error connecting to Cassandra:", err)
		return nil, err
	}

	err = session.Query(fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS notifications
				WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3}`)).Exec()
	if err != nil {
		logger.Println("Error creating keyspace:", err)
		return nil, err
	}
	session.Close()

	cluster.Keyspace = "notifications"
	cluster.Consistency = gocql.One
	session, err = cluster.CreateSession()
	if err != nil {
		logger.Println("Error connecting to notifications keyspace:", err)
		return nil, err
	}

	return &NotificationRepo{
		session: session,
		logger:  logger,
		tracer:  tracer,
	}, nil
}

func (repo *NotificationRepo) CloseSession() {
	repo.session.Close()
}

func (repo *NotificationRepo) CreateTables() {
	_, span := repo.tracer.Start(context.Background(), "NotificationRepo.CreateTables")
	defer span.End()
	err := repo.session.Query(`CREATE TABLE IF NOT EXISTS notifications (
		user_id TEXT,
		created_at TIMESTAMP,
		id UUID,
		message TEXT,
		status TEXT,
		PRIMARY KEY (user_id, created_at, id)
	) WITH CLUSTERING ORDER BY (created_at DESC);`).Exec()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		repo.logger.Println("Error creating notifications table with clustering:", err)
		return
	}

	err = repo.session.Query(`CREATE INDEX IF NOT EXISTS user_id_idx ON notifications (user_id)`).Exec()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		repo.logger.Println("Error creating index on user_id:", err)
	}
	span.SetStatus(codes.Ok, successfulFunc)
}

func (repo *NotificationRepo) Create(ctx context.Context, notification *model.Notification) error {
	_, span := repo.tracer.Start(ctx, "NotificationRepo.Create")
	defer span.End()
	location, err := time.LoadLocation("Europe/Budapest")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Println("Error loading time zone:", err)
		return err
	}
	notification.CreatedAt = time.Now().In(location)

	notification.ID, _ = gocql.RandomUUID()

	err = repo.session.Query(`
		INSERT INTO notifications (id, user_id, message, created_at, status)
		VALUES (?, ?, ?, ?, ?)`,
		notification.ID, notification.UserID, notification.Message, notification.CreatedAt, notification.Status).Exec()

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		repo.logger.Println("Error inserting notification:", err)
		return err
	}
	span.SetStatus(codes.Ok, successfulFunc)
	return nil
}

func (repo *NotificationRepo) GetByID(ctx context.Context, id gocql.UUID) (*model.Notification, error) {
	_, span := repo.tracer.Start(ctx, "NotificationRepo.GetByID")
	defer span.End()
	var notification model.Notification
	err := repo.session.Query(`
		SELECT id, user_id, message, created_at, status
		FROM notifications WHERE id = ?`, id).Consistency(gocql.One).Scan(
		&notification.ID, &notification.UserID, &notification.Message, &notification.CreatedAt, &notification.Status)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if err == gocql.ErrNotFound {
			return nil, fmt.Errorf("notification with ID %v not found", id)
		}
		repo.logger.Println("Error fetching notification:", err)
		return nil, err
	}
	location, err := time.LoadLocation("Europe/Budapest")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Println("Error loading time zone:", err)
		return nil, err
	}

	notification.CreatedAt = notification.CreatedAt.In(location)
	span.SetStatus(codes.Ok, successfulFunc)
	return &notification, nil
}

func (repo *NotificationRepo) GetByUserID(ctx context.Context, userID string) ([]*model.Notification, error) {
	_, span := repo.tracer.Start(ctx, "NotificationRepo.GetByUserID")
	defer span.End()
	var notifications []*model.Notification

	iter := repo.session.Query(`
		SELECT id, user_id, message, created_at, status
		FROM notifications 
		WHERE user_id = ? 
		`, userID).Iter()

	for {
		var notification model.Notification
		if !iter.Scan(&notification.ID, &notification.UserID, &notification.Message, &notification.CreatedAt, &notification.Status) {
			break
		}
		notifications = append(notifications, &notification)
	}

	if err := iter.Close(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		repo.logger.Println("Error closing iterator:", err)
		return nil, err
	}
	span.SetStatus(codes.Ok, successfulFunc)

	return notifications, nil
}

func (repo *NotificationRepo) UpdateStatus(ctx context.Context, createdAt time.Time, userID string, id gocql.UUID, status model.NotificationStatus) error {
	_, span := repo.tracer.Start(ctx, "NotificationRepo.UpdateStatus")
	defer span.End()
	err := repo.session.Query(`
        UPDATE notifications 
        SET status = ? 
        WHERE user_id = ? AND created_at = ? AND id = ?`,
		status, userID, createdAt, id).Exec()

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		repo.logger.Println("Error updating notification status:", err)
		return err
	}
	span.SetStatus(codes.Ok, successfulFunc)
	return nil
}

func (repo *NotificationRepo) Delete(ctx context.Context, id gocql.UUID) error {
	_, span := repo.tracer.Start(ctx, "NotificationRepo.Delete")
	defer span.End()
	err := repo.session.Query(`
		DELETE FROM notifications WHERE id = ?`, id).Exec()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		repo.logger.Println("Error deleting notification:", err)
		return err
	}
	span.SetStatus(codes.Ok, successfulFunc)
	return nil
}
