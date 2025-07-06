package handlers

import (
	"analytics-service/customLogger"
	"analytics-service/model"
	"analytics-service/repository"
	"context"
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"log"
	"net/http"
	"os"
)

// EventHandler processes events for both HTTP and internal event processing.
type EventHandler struct {
	repo       *repository.ESDBClient
	tracer     trace.Tracer
	logger     *log.Logger
	custLogger *customLogger.Logger
}

const (
	failedProcessEvent = "failed to process the event"
)

// NewEventHandler creates a new EventHandler with a given repository.
func NewEventHandler(repo *repository.ESDBClient, tracer trace.Tracer, logger *log.Logger) *EventHandler {
	return &EventHandler{repo: repo, tracer: tracer, logger: logger}
}

func ExtractTraceInfoMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *EventHandler) ProcessEventHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.Start(r.Context(), "EventHandler.ProcessEventHandler")
	defer span.End()
	var event model.Event
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Failed to decode event data", http.StatusBadRequest)
		return
	}

	message, err := h.processEvent(ctx, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, failedProcessEvent, http.StatusInternalServerError)
		return
	}

	if message != "" {
		span.SetStatus(codes.Ok, message)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(message))
	} else {
		span.RecordError(errors.New(failedProcessEvent))
		span.SetStatus(codes.Error, failedProcessEvent)
		http.Error(w, "Event type not handled", http.StatusBadRequest)
	}
}

func (h *EventHandler) GetEventsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.Start(r.Context(), "EventHandler.GetEventsHandler")
	defer span.End()
	// Extract the projectID variable from the URL
	vars := mux.Vars(r)
	projectID := vars["projectID"]
	if projectID == "" {
		span.RecordError(errors.New("projectID is required"))
		span.SetStatus(codes.Error, "projectID is required")
		http.Error(w, "Missing projectID parameter", http.StatusBadRequest)
		return
	}

	// Fetch events for the given project
	events, err := h.repo.GetEventsByProjectID(ctx, projectID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Failed to retrieve events", http.StatusInternalServerError)
		return
	}

	// Respond with a JSON array of events
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(events); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Failed to encode events", http.StatusInternalServerError)
	}
	span.SetStatus(codes.Ok, "")
}

func (h *EventHandler) processEvent(ctx context.Context, event model.Event) (string, error) {
	ctx, span := h.tracer.Start(ctx, "EventHandler.processEvent")
	defer span.End()

	if err := h.repo.StoreEvent(ctx, event.ProjectID, event); err != nil {
		return h.handleError(span, "Failed to store event", err)
	}

	var message string
	var subject string
	var message4nats interface{}

	switch event.Type {
	case model.ProjectCreatedType:
		message = "Successfully created a project"
		subject = "project.created"
		message4nats = h.createProjectCreatedMessage(event)

	case model.MemberAddedType:
		message = "Successfully added member to project"
		subject = "member.added"
		message4nats = h.createMemberAddedMessage(event)

	case model.MemberRemovedType:
		message = "Successfully removed member from project"
		subject = "member.removed"
		message4nats = h.createMemberRemovedMessage(event)

	case model.MemberAddedTaskType:
		message = "Successfully added member to task"
		subject = "member.task.added"
		message4nats = h.createMemberAddedTaskMessage(event)

	case model.MemberRemovedTaskType:
		message = "Successfully removed member from task"
		subject = "member.task.removed"
		message4nats = h.createMemberRemovedTaskMessage(event)

	case model.TaskCreatedType:
		message = "Successfully created task"
		subject = "task.created"
		message4nats = h.createTaskCreatedMessage(event)

	case model.TaskStatusChangedType:
		message = "Successfully changed task status"
		subject = "task.status.change"
		message4nats = h.createTaskStatusChangedMessage(event)

	case model.DocumentAddedType:
		message = "Successfully added document"

	default:
		return h.handleError(span, "Unhandled event type", errors.New("unhandled event type"))
	}

	if message4nats != nil {
		if err := h.sendNotification(ctx, subject, message4nats); err != nil {
			return h.handleError(span, "Failed to send notification", err)
		}
	}

	span.SetStatus(codes.Ok, "")
	return message, nil
}

func (h *EventHandler) handleError(span trace.Span, logMessage string, err error) (string, error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	log.Printf("%s: %v", logMessage, err)
	return "", err
}

func (h *EventHandler) createProjectCreatedMessage(event model.Event) interface{} {
	var projectCreatedEvent model.ProjectCreatedEvent
	h.unmarshalEvent(event, &projectCreatedEvent)

	return struct {
		ProjectID string `json:"projectId"`
		EndDate   string `json:"endDate"`
	}{
		ProjectID: event.ProjectID,
		EndDate:   projectCreatedEvent.EndDate,
	}
}

func (h *EventHandler) createMemberAddedMessage(event model.Event) interface{} {
	var memberAddedProjectEvent model.MemberAddedToProjectEvent
	h.unmarshalEvent(event, &memberAddedProjectEvent)

	return struct {
		ProjectID string `json:"projectId"`
		MemberID  string `json:"memberId"`
	}{
		ProjectID: event.ProjectID,
		MemberID:  memberAddedProjectEvent.MemberID,
	}
}

func (h *EventHandler) createMemberRemovedMessage(event model.Event) interface{} {
	var memberRemovedProjectEvent model.MemberRemovedFromProjectEvent
	h.unmarshalEvent(event, &memberRemovedProjectEvent)

	return struct {
		ProjectID string `json:"projectId"`
		MemberID  string `json:"memberId"`
	}{
		ProjectID: event.ProjectID,
		MemberID:  memberRemovedProjectEvent.MemberID,
	}
}

func (h *EventHandler) createMemberAddedTaskMessage(event model.Event) interface{} {
	var memberAddedTaskEvent model.MemberAddedToTaskEvent
	h.unmarshalEvent(event, &memberAddedTaskEvent)

	return struct {
		ProjectID string `json:"projectId"`
		TaskID    string `json:"taskId"`
		MemberID  string `json:"memberId"`
	}{
		ProjectID: event.ProjectID,
		TaskID:    memberAddedTaskEvent.TaskID,
		MemberID:  memberAddedTaskEvent.MemberID,
	}
}

func (h *EventHandler) createMemberRemovedTaskMessage(event model.Event) interface{} {
	var memberRemovedTaskEvent model.MemberRemovedFromTaskEvent
	h.unmarshalEvent(event, &memberRemovedTaskEvent)

	return struct {
		ProjectID string `json:"projectId"`
		TaskID    string `json:"taskId"`
		MemberID  string `json:"memberId"`
	}{
		ProjectID: event.ProjectID,
		TaskID:    memberRemovedTaskEvent.TaskID,
		MemberID:  memberRemovedTaskEvent.MemberID,
	}
}

func (h *EventHandler) createTaskCreatedMessage(event model.Event) interface{} {
	var taskCreated model.TaskCreatedEvent
	h.unmarshalEvent(event, &taskCreated)

	return struct {
		ProjectID string `json:"projectId"`
		TaskID    string `json:"taskId"`
	}{
		ProjectID: event.ProjectID,
		TaskID:    taskCreated.TaskID,
	}
}

func (h *EventHandler) createTaskStatusChangedMessage(event model.Event) interface{} {
	var taskStatusChanged model.TaskStatusChangedEvent
	h.unmarshalEvent(event, &taskStatusChanged)

	return struct {
		ProjectID string `json:"projectId"`
		TaskID    string `json:"taskId"`
		Status    string `json:"status"`
	}{
		ProjectID: event.ProjectID,
		TaskID:    taskStatusChanged.TaskID,
		Status:    taskStatusChanged.Status,
	}
}

func (h *EventHandler) unmarshalEvent(event model.Event, target interface{}) {
	eventJSON, err := json.Marshal(event.Event)
	if err != nil {
		log.Printf("Failed to marshal event: %v", err)
		return
	}
	if err := json.Unmarshal(eventJSON, target); err != nil {
		log.Printf("Failed to unmarshal event: %v", err)
	}
}

func (p *EventHandler) sendNotification(ctx context.Context, subject string, message interface{}) error {
	_, span := p.tracer.Start(ctx, "ProjectsHandler.SendNotification")
	defer span.End()
	nc, err := Conn()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Println("Error connecting to NATS:", err)
		p.logger.Println("Error connecting to NATS:", err)
		p.custLogger.Error(logrus.Fields{
			"error": err.Error(),
		}, "Failed to connect to NATS")
	}
	defer nc.Close()

	jsonMessage, err := json.Marshal(message)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Println("Error marshalling message:", err)
		p.logger.Println("Error marshalling message:", err)

	}

	err = nc.Publish(subject, jsonMessage)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Println("Error publishing message to NATS:", err)
		p.logger.Println("Error publishing message to NATS:", err)
	}

	p.logger.Println("Notification sent:", subject)
	return nil
}

func Conn() (*nats.Conn, error) {
	connection := os.Getenv("NATS_URL")
	conn, err := nats.Connect(connection)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return conn, nil
}
