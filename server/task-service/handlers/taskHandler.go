package handlers

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/colinmarc/hdfs/v2"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/sony/gobreaker"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"task--service/client"
	"task--service/customLogger"
	"task--service/domain"
	"task--service/model"
	"task--service/repositories"
	"time"
)

type TasksHandler struct {
	logger       *log.Logger
	repo         *repositories.TaskRepository
	documentRepo *repositories.TaskDocumentRepository
	natsConn     *nats.Conn
	tracer       trace.Tracer
	userClient   client.UserClient
	custLogger   *customLogger.Logger
}

type KeyTask struct{}
type KeyId struct{}
type KeyRole struct{}

const (
	receivedReq    = "Received %s request for %s"
	eventFail      = "Failed to send event to analytics service"
	dbErr          = "Database exception: "
	extractedId    = "Extracted project ID from request"
	failFetch      = "Failed to fetch tasks"
	encodeErr      = "Failed to encode response"
	contentType    = "Content-Type"
	appJson        = "application/json"
	invalidAction  = "Invalid action"
	successfulTask = "Successfully updated task"
	taskData       = "Task data is missing or invalid"
	missingId      = "User ID is missing or invalid"
)

func NewTasksHandler(l *log.Logger, r *repositories.TaskRepository, docRepo *repositories.TaskDocumentRepository, natsConn *nats.Conn, tracer trace.Tracer, userClient client.UserClient, custLogger *customLogger.Logger) *TasksHandler {
	return &TasksHandler{
		logger:       l,
		repo:         r,
		documentRepo: docRepo,
		natsConn:     natsConn,
		tracer:       tracer,
		userClient:   userClient,
		custLogger:   custLogger,
	}
}

func ExtractTraceInfoMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (t *TasksHandler) PostTask(rw http.ResponseWriter, h *http.Request) {
	ctx, span := t.tracer.Start(h.Context(), "TaskHandler.PostTask")
	defer span.End()
	task := h.Context().Value(KeyTask{}).(*model.Task)
	t.logger.Printf(receivedReq, h.Method, h.URL.Path)
	t.custLogger.Info(nil, fmt.Sprintf(receivedReq, h.Method, h.URL.Path))

	// Preuzimanje Task-a iz Context-a
	task, ok := h.Context().Value(KeyTask{}).(*model.Task)
	if !ok || task == nil {
		errMsg := "Invalid task data in context"
		t.logger.Printf(errMsg)
		t.custLogger.Error(nil, errMsg)
		http.Error(rw, "Invalid task data", http.StatusBadRequest)
		return
	}
	t.custLogger.Info(logrus.Fields{"taskID": task.ID}, "Task data retrieved successfully")

	// Ubacivanje Task-a u repozitorijum
	err := t.repo.Insert(ctx, task)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		errMsg := "Unable to create task: " + err.Error()
		t.logger.Printf(errMsg)
		t.custLogger.Error(logrus.Fields{"taskID": task.ID}, errMsg)
		http.Error(rw, "Unable to create task", http.StatusInternalServerError)
		return
	}
	span.SetStatus(codes.Ok, "Successfully created task")
	t.custLogger.Info(logrus.Fields{"taskID": task.ID}, "Task created successfully")
	t.custLogger.Info(logrus.Fields{"projectID": task.ProjectID}, "ProjectID")

	currentTime := time.Now().Add(1 * time.Hour)
	formattedTime := currentTime.Format(time.RFC3339)

	event := map[string]interface{}{
		"type": "TaskCreated",
		"time": formattedTime,
		"event": map[string]interface{}{
			"taskId":    task.ID,
			"projectId": task.ProjectID,
		},
		"projectId": task.ProjectID,
	}

	// Send the event to the analytic service
	if err := t.sendEventToAnalyticsService(ctx, event); err != nil {
		http.Error(rw, eventFail, http.StatusInternalServerError)
		return
	}

	taskIDStr := task.ID.Hex()
	// Slanje odgovora
	rw.WriteHeader(http.StatusCreated)
	response := map[string]interface{}{
		"message": "Task created successfully",
		"task":    task,
		"taskId":  taskIDStr,
	}
	err = json.NewEncoder(rw).Encode(response)
	if err != nil {
		errMsg := "Error writing response: " + err.Error()
		t.logger.Printf(errMsg)
		t.custLogger.Error(nil, errMsg)
		return
	}
	t.custLogger.Info(nil, "Task creation response sent successfully")
}

func (t *TasksHandler) GetAllTask(rw http.ResponseWriter, h *http.Request) {
	ctx, span := t.tracer.Start(h.Context(), "TaskHandler.GetAllTask")
	defer span.End()
	t.logger.Printf(receivedReq, h.Method, h.URL.Path)
	t.custLogger.Info(nil, fmt.Sprintf(receivedReq, h.Method, h.URL.Path))

	// Preuzimanje svih zadataka iz repozitorijuma
	projects, err := t.repo.GetAllTask(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		t.logger.Print(dbErr, err)
		errMsg := dbErr + err.Error()
		t.logger.Print(errMsg)
		t.custLogger.Error(nil, errMsg)
		http.Error(rw, "Internal server error", http.StatusInternalServerError)
		return
	}
	t.custLogger.Info(nil, "All tasks retrieved successfully")

	// Konvertovanje u JSON i slanje odgovora
	err = projects.ToJSON(rw)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, "Unable to convert to json", http.StatusInternalServerError)
		errMsg := "Unable to convert to JSON: " + err.Error()
		t.custLogger.Error(nil, errMsg)
		http.Error(rw, "Unable to convert to JSON", http.StatusInternalServerError)
		return
	}
	t.custLogger.Info(nil, "Tasks successfully converted to JSON and sent in response")
	span.SetStatus(codes.Ok, "Successfully retrieved all tasks")
}

func (t *TasksHandler) GetAllTasksByProjectId(rw http.ResponseWriter, h *http.Request) {
	ctx, span := t.tracer.Start(h.Context(), "TaskHandler.GetAllTasksByProjectId")
	defer span.End()
	t.logger.Printf(receivedReq, h.Method, h.URL.Path)
	t.custLogger.Info(nil, fmt.Sprintf(receivedReq, h.Method, h.URL.Path))

	// Ekstrakcija projectID iz URL-a
	vars := mux.Vars(h)
	projectID := vars["projectId"]
	t.custLogger.Info(logrus.Fields{"projectID": projectID}, extractedId)

	// Preuzimanje zadataka za dati projectID
	tasks, err := t.repo.GetAllByProjectId(ctx, projectID)

	//http.Error(rw, "Service unavailable for testing", http.StatusServiceUnavailable)
	//return

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		t.logger.Print(dbErr, err)
		errMsg := dbErr
		t.logger.Print(errMsg, err)
		t.custLogger.Error(logrus.Fields{"projectID": projectID}, errMsg+": "+err.Error())
		http.Error(rw, failFetch, http.StatusInternalServerError)
		return
	}
	t.custLogger.Info(logrus.Fields{"projectID": projectID, "taskCount": len(tasks)}, "Tasks fetched successfully")

	// Enkodovanje zadataka u JSON i slanje odgovora
	if err := json.NewEncoder(rw).Encode(tasks); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, encodeErr, http.StatusInternalServerError)
		errMsg := encodeErr
		t.logger.Printf("%s: %v", errMsg, err)
		t.custLogger.Error(logrus.Fields{"projectID": projectID}, errMsg+": "+err.Error())
		http.Error(rw, errMsg, http.StatusInternalServerError)
		return
	}
	span.SetStatus(codes.Ok, "Successfully retrieved all tasks")

	t.custLogger.Info(logrus.Fields{"projectID": projectID}, "Tasks successfully encoded and sent in response")
}

func (t *TasksHandler) GetAllTasksDetailsByProjectId(rw http.ResponseWriter, h *http.Request) {
	ctx, span := t.tracer.Start(h.Context(), "TaskHandler.GetAllTasksDetailsByProjectId")
	defer span.End()
	t.logger.Printf(receivedReq, h.Method, h.URL.Path)
	t.custLogger.Info(nil, fmt.Sprintf(receivedReq, h.Method, h.URL.Path))

	// Step 1: Get the project ID from the URL
	vars := mux.Vars(h)
	projectID := vars["projectId"]
	t.custLogger.Info(logrus.Fields{"projectID": projectID}, extractedId)

	// Step 2: Validate token in cookies
	cookie, err := h.Cookie("auth_token")
	if err != nil {
		errMsg := "No token found in cookie"
		t.logger.Println(errMsg, err)
		t.custLogger.Error(nil, errMsg+": "+err.Error())
		http.Error(rw, errMsg, http.StatusUnauthorized)
		return
	}
	t.custLogger.Info(nil, "Authorization token found in cookie")

	// Step 3: Fetch tasks for the given project
	tasks, err := t.repo.GetAllByProjectId(ctx, projectID)
	if err != nil {
		errMsg := dbErr
		t.logger.Print(errMsg, err)
		t.custLogger.Error(logrus.Fields{"projectID": projectID}, errMsg+": "+err.Error())
		http.Error(rw, failFetch, http.StatusInternalServerError)
		return
	}
	t.custLogger.Info(logrus.Fields{"projectID": projectID, "taskCount": len(tasks)}, "Tasks fetched successfully")

	// Step 4: Prepare a slice to store tasks with user details
	var tasksWithUserDetails []client.TaskDetails

	// Step 5: Fetch user details for each task
	for _, task := range tasks {
		t.custLogger.Info(logrus.Fields{"taskID": task.ID}, "Fetching user details for task")
		usersDetails, err := t.userClient.GetByIdsWithCookies(task.UserIDs, cookie)
		if err != nil {
			errMsg := "Error fetching user details"
			t.logger.Printf("%s for task ID '%s': %v", errMsg, task.ID, err)
			t.custLogger.Error(logrus.Fields{"taskID": task.ID}, errMsg+": "+err.Error())
			http.Error(rw, "Error fetching user details for tasks", http.StatusInternalServerError)
			return
		}
		t.custLogger.Info(logrus.Fields{"taskID": task.ID, "userCount": len(usersDetails)}, "User details fetched successfully")

		// Step 6: Map task to TaskDetails and include user details
		taskDetails := client.TaskDetails{
			ID:           task.ID,
			ProjectID:    task.ProjectID,
			Name:         task.Name,
			Description:  task.Description,
			Status:       task.Status,
			CreatedAt:    task.CreatedAt,
			UpdatedAt:    task.UpdatedAt,
			UserIDs:      task.UserIDs,
			Users:        usersDetails,
			Dependencies: task.Dependencies,
			Blocked:      task.Blocked,
		}

		// Add the task with user details to the result slice
		tasksWithUserDetails = append(tasksWithUserDetails, taskDetails)
	}

	t.logger.Printf("Tasks with details is: %+v", tasksWithUserDetails)

	// Step 7: Return the tasks with user details in the response
	if err := json.NewEncoder(rw).Encode(tasksWithUserDetails); err != nil {
		errMsg := encodeErr
		t.logger.Printf("%s: %v", errMsg, err)
		t.custLogger.Error(nil, errMsg+": "+err.Error())
		http.Error(rw, errMsg, http.StatusInternalServerError)
		return
	}
	t.custLogger.Info(logrus.Fields{"projectID": projectID}, "Tasks with user details successfully returned in response")
}

func (t *TasksHandler) GetStatusForTask(rw http.ResponseWriter, h *http.Request) {
	ctx, span := t.tracer.Start(h.Context(), "TaskHandler.GetAllTasksDetailsByProjectId")
	defer span.End()
	t.logger.Printf(receivedReq, h.Method, h.URL.Path)
	t.custLogger.Info(nil, fmt.Sprintf(receivedReq, h.Method, h.URL.Path))

	vars := mux.Vars(h)
	taskID := vars["taskId"]
	t.custLogger.Info(logrus.Fields{"taskId": taskID}, extractedId)

	task, err := t.repo.GetByID(ctx, taskID)
	if err != nil {
		errMsg := dbErr
		t.logger.Print(errMsg, err)
		t.custLogger.Error(logrus.Fields{"taskID": taskID}, errMsg+": "+err.Error())
		http.Error(rw, failFetch, http.StatusInternalServerError)
		return
	}

	status := task.Status
	rw.Header().Set(contentType, appJson)
	rw.WriteHeader(http.StatusOK)
	_, err = rw.Write([]byte(fmt.Sprintf(`"%s"`, status)))
	if err != nil {
		t.logger.Printf("Failed to write response: %v", err)
		t.custLogger.Error(nil, "Failed to write response: "+err.Error())
	}

}

func (t *TasksHandler) HandleProjectDeleted(ctx context.Context, projectID string) {
	ctx, span := t.tracer.Start(ctx, "TaskHandler.HandleProjectDeleted")
	defer span.End()

	err := t.repo.UpdateAllTasksByProjectId(ctx, projectID, true)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		t.logger.Printf("Failed to delete tasks for project %s: %v", projectID, err)

		_ = t.natsConn.Publish("TaskDeletionFailed", []byte(projectID))
	}
	t.logger.Printf("Successfully updated all tasks for project %s", projectID)

	err = t.natsConn.Publish("TasksDeleted", []byte(projectID))
	if err != nil {
		t.logger.Printf("Failed to publish TasksDeleted event for project %s: %v", projectID, err)
	} else {
		t.logger.Printf("Successfully published TasksDeleted event for project %s", projectID)
	}

	//_ = t.natsConn.Publish("TaskDeletionFailed", []byte(projectID)) // uncomment this, and comment out the code above to test 'rollback' function

	span.SetStatus(codes.Ok, "Successfully updated all tasks")
}

func (t *TasksHandler) DeletedTasks(ctx context.Context, projectID string) {
	_, span := t.tracer.Start(context.Background(), "TaskHandler.HandleProjectDeleted")
	defer span.End()

	//	err := t.repo.DeleteAllTasksByProjectId(projectID)
	//=======
	err := t.repo.DeleteAllTasksByProjectId(ctx, projectID)
	//>>>>>>> b38a495c0faab5935c6effddd29991a392a36c70
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		t.logger.Printf("Failed to delete tasks for project %s: %v", projectID, err)

		_ = t.natsConn.Publish("TaskDeletionFailed", []byte(projectID))
	}
	t.logger.Printf("Successfully deleted all tasks for project %s", projectID)

	/*	err = t.natsConn.Publish("TasksDeleted", []byte(projectID))
		if err != nil {
			t.logger.Printf("Failed to publish TasksDeleted event for project %s: %v", projectID, err)
		}
	*/
	//_ = t.natsConn.Publish("TaskDeletionFailed", []byte(projectID)) // uncomment this, and comment out the code above to test 'rollback' function

	span.SetStatus(codes.Ok, "Successfully deleted all tasks")
}

func (t *TasksHandler) MiddlewareContentTypeSet(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, h *http.Request) {
		t.logger.Println("Method [", h.Method, "] - Hit path :", h.URL.Path)
		rw.Header().Add(contentType, appJson)
		next.ServeHTTP(rw, h)
	})
}

func (t *TasksHandler) MiddlewareTaskDeserialization(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, h *http.Request) {
		task := &model.Task{}
		err := task.FromJSON(h.Body)
		if err != nil {
			http.Error(rw, "Unable to decode json", http.StatusBadRequest)
			t.logger.Println("Error decoding JSON:", err)
			return
		}

		ctx := context.WithValue(h.Context(), KeyTask{}, task)
		h = h.WithContext(ctx)

		next.ServeHTTP(rw, h)
	})
}

func (uh *TasksHandler) MiddlewareCheckRoles(allowedRoles []string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, h *http.Request) {
		role, ok := h.Context().Value(KeyRole{}).(string)
		if !ok {
			http.Error(rw, "Forbidden", http.StatusForbidden)
			uh.logger.Println("Role not found in context")
			return
		}

		allowed := false
		for _, r := range allowedRoles {
			if role == r {
				allowed = true
				break
			}
		}

		if !allowed {
			http.Error(rw, "Forbidden", http.StatusForbidden)
			uh.logger.Println("Role validation failed: missing permissions")
			return
		}

		next.ServeHTTP(rw, h)
	})
}

func (p *TasksHandler) MiddlewareExtractUserFromCookie(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, h *http.Request) {
		p.logger.Printf(receivedReq, h.Method, h.URL.Path)
		p.custLogger.Info(nil, fmt.Sprintf(receivedReq, h.Method, h.URL.Path))

		// Ekstrakcija auth_token iz kolačića
		cookie, err := h.Cookie("auth_token")
		if err != nil {
			errMsg := "No token found in cookie"
			p.logger.Println(errMsg, err)
			p.custLogger.Error(nil, errMsg+": "+err.Error())
			http.Error(rw, errMsg, http.StatusUnauthorized)
			return
		}
		p.custLogger.Info(nil, "Authorization token found in cookie")

		// Verifikacija tokena preko korisničkog servisa
		userID, role, err := p.verifyTokenWithUserService(h.Context(), cookie.Value)
		if err != nil {
			errMsg := "Invalid token"
			p.logger.Println(errMsg, err)
			p.custLogger.Error(nil, errMsg+": "+err.Error())
			http.Error(rw, errMsg, http.StatusUnauthorized)
			return
		}
		p.custLogger.Info(logrus.Fields{"userID": userID, "role": role}, "Token verified successfully")

		// Dodavanje korisničkih podataka u kontekst
		ctx := context.WithValue(h.Context(), KeyId{}, userID)
		ctx = context.WithValue(ctx, KeyRole{}, role)
		h = h.WithContext(ctx)

		// Nastavak do sledećeg handler-a
		p.custLogger.Info(logrus.Fields{"userID": userID, "role": role}, "Context updated with user ID and role, proceeding to next handler")
		next.ServeHTTP(rw, h)
	})
}

func (t *TasksHandler) verifyTokenWithUserService(ctx context.Context, token string) (string, string, error) {
	ctx, span := t.tracer.Start(ctx, "ProjectsHandler.verifyTokenWithUserService")
	defer span.End()

	userServiceUrl, err := t.getUserServiceURL()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", "", err
	}

	req, err := t.createRequest(ctx, userServiceUrl, token, span)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", "", err
	}

	cl, err := createTLSClient()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", "", err
	}

	circuitBreaker := t.createCircuitBreaker()
	r := retrier.New(retrier.ConstantBackoff(3, 1000*time.Millisecond), retrier.WhitelistClassifier{domain.ErrRespTmp{}})

	resp, err := t.executeRequestWithRetries(ctx, r, circuitBreaker, cl, req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", "", err
	}

	return t.handleResponse(resp, span)
}

func (t *TasksHandler) getUserServiceURL() (string, error) {
	linkToUserService := os.Getenv("LINK_TO_USER_SERVICE")
	return fmt.Sprintf("%s/validate-token", linkToUserService), nil
}

func (t *TasksHandler) createRequest(ctx context.Context, userServiceUrl,
	token string, span trace.Span) (*http.Request, error) {
	reqBody := fmt.Sprintf(`{"token": "%s"}`, token)
	req, err := http.NewRequestWithContext(ctx, "POST", userServiceUrl, strings.NewReader(reqBody))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	req.Header.Set(contentType, appJson)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	return req, nil
}

func (t *TasksHandler) createCircuitBreaker() *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "UserServiceCircuitBreaker",
		MaxRequests: 5,
		Interval:    0,
		Timeout:     5 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures > 2
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			t.logger.Printf("Circuit Breaker '%s' changed from '%s' to '%s'", name, from, to)
		},
		IsSuccessful: func(err error) bool {
			if err == nil {
				return true
			}
			_, ok := err.(domain.ErrRespTmp)
			return !ok
		},
	})
}

func (t *TasksHandler) executeRequestWithRetries(ctx context.Context, r *retrier.Retrier,
	circuitBreaker *gobreaker.CircuitBreaker, client *http.Client, req *http.Request) (*http.Response, error) {
	var resp *http.Response
	err := r.RunCtx(ctx, func(ctx context.Context) error {
		t.logger.Println("Attempting execute request")
		_, err := circuitBreaker.Execute(func() (interface{}, error) {
			resp, err := client.Do(req)
			if err != nil {
				return nil, err
			}
			return t.checkResponse(resp)
		})
		return err
	})
	if err != nil {
		t.logger.Printf("Error during execute request: %v", err)
		return nil, err
	}

	return resp, nil
}

func (t *TasksHandler) checkResponse(resp *http.Response) (interface{}, error) {
	if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
		return nil, domain.ErrRespTmp{
			URL:        resp.Request.URL.String(),
			Method:     resp.Request.Method,
			StatusCode: resp.StatusCode,
		}
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return resp, nil
}

func (t *TasksHandler) handleResponse(resp *http.Response, span trace.Span) (string, string, error) {
	defer resp.Body.Close()

	var result struct {
		UserID string `json:"user_id"`
		Role   string `json:"role"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		span.RecordError(err)
		t.recordError(err, "Error decoding response", span)
		return "", "", fmt.Errorf("error decoding response: %w", err)
	}

	t.logger.Printf("ROLE IS %s", result.Role)
	span.SetStatus(codes.Ok, "Successfully validated token")
	return result.UserID, result.Role, nil
}

func (t *TasksHandler) recordError(err error, message string, span trace.Span) {
	t.logger.Printf("%s : %v", message, err)
	span.RecordError(err)
	span.SetStatus(codes.Error, message)
}

func createTLSClient() (*http.Client, error) {
	caCert, err := ioutil.ReadFile("/app/cert.crt")
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, err
	}

	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	transport := &http.Transport{
		TLSClientConfig:     tlsConfig,
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 10,
		MaxConnsPerHost:     10,
	}

	c := &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}

	return c, nil
}

func (t *TasksHandler) LogTaskMemberChange(rw http.ResponseWriter, h *http.Request) {
	ctx, span := t.tracer.Start(h.Context(), "TaskHandler.LogTaskMemberChange")
	defer span.End()

	vars := mux.Vars(h)
	taskID := vars["taskId"]
	action := vars["action"] // Can be "add" or "remove"
	userID := vars["userId"]

	t.logger.Println("User  id is " + userID)
	t.logger.Println("Action is " + action)

	if err := t.validateAction(span, action, rw); err != nil {
		return
	}

	task, err := t.repo.GetByID(ctx, taskID)
	if err != nil {
		err := t.handleError(span, err, rw, "Task not found", http.StatusNotFound)
		if err != nil {
			return
		}

	}

	if err := t.processAction(ctx, span, action, task, userID, rw); err != nil {
		return
	}

	if err := t.repo.Update(ctx, task); err != nil {
		err := t.handleError(span, err, rw, "Failed to update task", http.StatusInternalServerError)
		if err != nil {
			return
		}

	}

	t.ProcessTaskMemberActivity(ctx)

	rw.Header().Set(contentType, appJson)
	rw.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(rw).Encode(map[string]string{
		"message": "Task member change logged and task updated successfully",
	})
	if err != nil {
		return
	}

	span.SetStatus(codes.Ok, successfulTask)
}

func (t *TasksHandler) validateAction(span trace.Span, action string, rw http.ResponseWriter) error {
	if action != "add" && action != "remove" {
		span.RecordError(errors.New(invalidAction))
		span.SetStatus(codes.Error, invalidAction)
		http.Error(rw, invalidAction, http.StatusBadRequest)
		return errors.New(invalidAction)
	}
	return nil
}

func (t *TasksHandler) processAction(ctx context.Context, span trace.Span, action string, task *model.Task, userID string, rw http.ResponseWriter) error {
	if action == "add" {
		return t.addTaskMember(ctx, span, task, userID, rw)
	} else {
		return t.removeTaskMember(ctx, span, task, userID, rw)
	}
}

func (t *TasksHandler) addTaskMember(ctx context.Context, span trace.Span, task *model.Task, userID string, rw http.ResponseWriter) error {
	if !t.isUserInProject(ctx, task.ProjectID, userID) {
		return t.handleError(span, errors.New("Invalid userId"), rw, "User  not part of the project", http.StatusForbidden)
	}

	if contains(task.UserIDs, userID) {
		return t.handleError(span, errors.New("user is already a member of this task"), rw, "User  is already a member of this task", http.StatusConflict)
	}

	activity := model.TaskMemberActivity{
		TaskID:    task.ID.Hex(),
		UserID:    userID,
		Action:    "add",
		Timestamp: time.Now(),
		Processed: false,
	}

	if err := t.repo.InsertTaskMemberActivity(ctx, &activity); err != nil {
		return t.handleError(span, err, rw, "Failed to log task member change", http.StatusInternalServerError)
	}

	task.UserIDs = append(task.UserIDs, userID)
	return t.publishEvent(ctx, span, "task.joined", userID, task, "MemberAddedTask", rw)
}

func (t *TasksHandler) removeTaskMember(ctx context.Context, span trace.Span, task *model.Task, userID string, rw http.ResponseWriter) error {
	if task.Status == model.Completed {
		return t.handleError(span, errors.New("cannot remove task"), rw, "Cannot remove member from a completed task", http.StatusForbidden)
	}

	if !contains(task.UserIDs, userID) {
		return t.handleError(span, errors.New("Invalid userId"), rw, "User  is not a member of this task", http.StatusBadRequest)
	}

	activity := model.TaskMemberActivity{
		TaskID:    task.ID.Hex(),
		UserID:    userID,
		Action:    "remove",
		Timestamp: time.Now(),
		Processed: false,
	}

	if err := t.repo.InsertTaskMemberActivity(ctx, &activity); err != nil {
		return t.handleError(span, err, rw, "Failed to log task member change", http.StatusInternalServerError)
	}

	task.UserIDs = remove(task.UserIDs, userID)
	return t.publishEvent(ctx, span, "task.removed", userID, task, "MemberRemovedTask", rw)
}

func (t *TasksHandler) publishEvent(ctx context.Context, span trace.Span, subject, userID string, task *model.Task, eventType string, rw http.ResponseWriter) error {
	nc, err := Conn()
	if err != nil {
		return t.handleError(span, err, rw, "Failed to connect to message broker", http.StatusInternalServerError)
	}
	defer nc.Close()

	message := struct {
		UserID   string `json:"userId"`
		TaskName string `json:"taskName"`
	}{
		UserID:   userID,
		TaskName: task.Name,
	}

	jsonMessage, err := json.Marshal(message)
	if err != nil {
		return t.handleError(span, err, rw, "Error marshalling message", http.StatusInternalServerError)
	}

	if err := nc.Publish(subject, jsonMessage); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Println("Error publishing message to NATS:", err)
	}

	currentTime := time.Now().Add(1 * time.Hour)
	formattedTime := currentTime.Format(time.RFC3339)

	event := map[string]interface{}{
		"type": eventType,
		"time": formattedTime,
		"event": map[string]interface{}{
			"memberId": userID,
			"taskId":   task.ID,
		},
		"projectId": task.ProjectID,
	}

	if err := t.sendEventToAnalyticsService(ctx, event); err != nil {
		http.Error(rw, eventFail, http.StatusInternalServerError)
		return err
	}

	t.logger.Println("a message has been sent")
	return nil
}

func (t *TasksHandler) handleError(span trace.Span, err error, rw http.ResponseWriter, message string, statusCode int) error {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	http.Error(rw, message, statusCode)
	t.logger.Println(message, ":", err)
	return err
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

func (t *TasksHandler) ProcessTaskMemberActivity(ctx context.Context) {
	ctx, span := t.tracer.Start(ctx, "TaskHandler.ProcessTaskMemberActivity")
	defer span.End()

	activities, err := t.repo.GetUnprocessedActivities(ctx)
	if err != nil {
		err := t.returnRecordedError(span, err, "Error fetching unprocessed changes")
		if err != nil {
			return
		}

	}

	for _, activity := range activities {
		if err := t.processActivity(ctx, span, activity); err != nil {
			err := t.returnRecordedError(span, err, "Error processing activity")
			if err != nil {
				return
			}
		}
	}

	span.SetStatus(codes.Ok, successfulTask)
}

func (t *TasksHandler) processActivity(ctx context.Context, span trace.Span, activity model.TaskMemberActivity) error {
	task, err := t.repo.GetByID(ctx, activity.TaskID)
	if err != nil {
		return t.returnRecordedError(span, err, "Error fetching task for change")
	}

	if err := t.updateTaskMembers(task, activity); err != nil {
		return err
	}

	if err := t.repo.Update(ctx, task); err != nil {
		return t.returnRecordedError(span, err, "Error updating task")
	}

	if err := t.repo.MarkChangeAsProcessed(ctx, activity.ID); err != nil {
		return t.returnRecordedError(span, err, "Error marking change as processed")
	}

	return nil
}

func (t *TasksHandler) updateTaskMembers(task *model.Task, activity model.TaskMemberActivity) error {
	switch activity.Action {
	case "add":
		if !contains(task.UserIDs, activity.UserID) {
			task.UserIDs = append(task.UserIDs, activity.UserID)
		}
	case "remove":
		if contains(task.UserIDs, activity.UserID) {
			task.UserIDs = remove(task.UserIDs, activity.UserID)
		}
	default:
		return errors.New("invalid action")
	}
	return nil
}

func (t *TasksHandler) returnRecordedError(span trace.Span, err error, message string) error {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	t.logger.Println(message, ":", err)
	return err
}

func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func remove(slice []string, item string) []string {
	result := []string{}
	for _, v := range slice {
		if v != item {
			result = append(result, v)
		}
	}
	return result
}

func (t *TasksHandler) isUserInProject(ctx context.Context, projectID, userID string) bool {
	ctx, span := t.tracer.Start(ctx, "TaskHandler.isUserInProject")
	defer span.End()

	linkToProjectService := os.Getenv("LINK_TO_PROJECT_SERVICE")
	projectServiceURL := fmt.Sprintf("%s/projects/%s/users/%s/check", linkToProjectService, projectID, userID)

	clientToDo, err := createTLSClient()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		t.logger.Println("Error creating TLS client:", err)
		return false
	}

	resp, err := clientToDo.Get(projectServiceURL)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		t.logger.Println("Error making GET request:", err)
		return false
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(resp.Header))
	defer resp.Body.Close()
	t.logger.Println("Response code: " + resp.Status)
	switch resp.StatusCode {
	case http.StatusOK:
		return true
	case http.StatusNotFound:
		return false
	default:
		t.logger.Println("Unexpected status code:", resp.StatusCode)
		span.SetStatus(codes.Ok, "Function is working with unexpected response")
		return false
	}
}

func (th *TasksHandler) HandleStatusUpdate(rw http.ResponseWriter, req *http.Request) {
	ctx, span := th.tracer.Start(req.Context(), "TaskHandler.HandleStatusUpdate")
	defer span.End()

	th.logger.Println("Received request to update task status")
	th.custLogger.Info(nil, "Received request to update task status")

	requestBody, err := th.parseRequestBody(req, span, rw)
	if err != nil {
		return
	}

	task, err := th.repo.GetByID(ctx, requestBody.ID)
	if err != nil {
		err := th.handleError(span, err, rw, "Failed to find task", http.StatusNotFound)
		if err != nil {
			return
		}

	}

	if err := th.checkTaskBlocked(task, rw); err != nil {
		return
	}

	if err := th.updateTaskStatus(ctx, task, requestBody.Status, rw, span); err != nil {
		return
	}

	if err := th.handleDependencies(ctx, task, rw); err != nil {
		return
	}

	if err := th.publishStatusUpdate(ctx, task); err != nil {
		th.logger.Println("Error publishing status update:", err)
		http.Error(rw, "Failed to notify task members", http.StatusInternalServerError)
		return
	}

	if err := th.sendAnalyticsEvent(ctx, task, req); err != nil {
		http.Error(rw, eventFail, http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusOK)
	th.logger.Println("Task status updated successfully")
	span.SetStatus(codes.Ok, successfulTask)
	th.custLogger.Info(logrus.Fields{"taskID": task.ID, "status": task.Status}, "Response sent successfully")
}

func (th *TasksHandler) parseRequestBody(req *http.Request, span trace.Span, rw http.ResponseWriter) (struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}, error) {
	var requestBody struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	err := json.NewDecoder(req.Body).Decode(&requestBody)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Invalid request body")
		http.Error(rw, "Invalid request body", http.StatusBadRequest)
		th.logger.Println("Failed to parse request body:", err)
		th.custLogger.Error(nil, "Invalid request body: "+err.Error())
		return requestBody, err
	}
	th.custLogger.Info(logrus.Fields{"taskID": requestBody.ID, "status": requestBody.Status}, "Parsed request body successfully")
	return requestBody, nil
}

func (th *TasksHandler) checkTaskBlocked(task *model.Task, rw http.ResponseWriter) error {
	if task.Blocked {
		th.logger.Println("Task is blocked and cannot change status!")
		th.custLogger.Info(logrus.Fields{"taskID": task.ID, "status": task.Status, "blocked": task.Blocked}, "Task is blocked and cannot change status")
		http.Error(rw, taskData, http.StatusBadRequest)
		return errors.New("task is blocked")
	}
	return nil
}

func (th *TasksHandler) updateTaskStatus(ctx context.Context, task *model.Task, status string, rw http.ResponseWriter, span trace.Span) error {
	if strings.TrimSpace(status) == "Completed" {
		if err := th.repo.UnblockDependencies(task); err != nil {
			th.logger.Println("Error updating blocked flag of dependent tasks:", err)
			http.Error(rw, "Failed to update blocked flag of dependent tasks", http.StatusInternalServerError)
			return err
		}
	}

	task.Status = model.TaskStatus(status)

	userID, ok := ctx.Value(KeyId{}).(string)
	if !ok || userID == "" {
		th.logger.Println("There is no id")
		http.Error(rw, "There is no id", http.StatusBadRequest)
		return errors.New("user ID is missing")
	}

	if err := th.repo.UpdateStatus(ctx, task, userID); err != nil {
		return th.handleError(span, err, rw, "Failed to update task status", http.StatusInternalServerError)

	}

	th.custLogger.Info(logrus.Fields{"taskID": task.ID, "status": task.Status}, "Task status updated successfully")
	return nil
}

func (th *TasksHandler) handleDependencies(ctx context.Context, task *model.Task, rw http.ResponseWriter) error {
	if string(task.Status) == "Completed" {
		for _, id := range task.Dependencies {
			if err := th.updateDependentTask(ctx, id); err != nil {
				http.Error(rw, "Error updating blocked flag for task", http.StatusInternalServerError)
				return err
			}
		}
	}
	return nil
}

func (th *TasksHandler) updateDependentTask(ctx context.Context, id string) error {
	dependentTask, err := th.repo.GetByID(ctx, id)
	if err != nil {
		th.logger.Println("Error fetching dependent task with ID", id, ":", err)
		return err
	}

	if err := th.repo.UpdateFlag(ctx, dependentTask, false); err != nil {
		th.logger.Println("Error updating blocked flag for task ID", id, ":", err)
		return err
	}

	event := TaskBlockedEvent{
		TaskID:  dependentTask.ID.Hex(),
		Blocked: false,
	}
	eventData, err := json.Marshal(event)
	if err != nil {
		return err
	}

	if err := th.natsConn.Publish("TaskBlocked", eventData); err != nil {
		return err
	}

	th.logger.Println("Successfully updated blocked flag to false!")
	return nil
}

func (th *TasksHandler) sendAnalyticsEvent(ctx context.Context, task *model.Task, req *http.Request) error {
	currentTime := time.Now().Add(1 * time.Hour)
	formattedTime := currentTime.Format(time.RFC3339)
	id, ok := req.Context().Value(KeyId{}).(string)
	if !ok || id == "" {
		return errors.New(missingId)
	}

	event := map[string]interface{}{
		"type": "TaskStatusChanged",
		"time": formattedTime,
		"event": map[string]interface{}{
			"taskId":    task.ID,
			"projectId": task.ProjectID,
			"status":    task.Status,
			"changedBy": id,
		},
		"projectId": task.ProjectID,
	}

	if err := th.sendEventToAnalyticsService(ctx, event); err != nil {
		return err
	}
	return nil
}

func (p *TasksHandler) sendEventToAnalyticsService(ctx context.Context, event interface{}) error {
	ctx, span := p.tracer.Start(ctx, "TaskHandler.SendEventToAnalyticsService")
	defer span.End()
	linkToUserServer := os.Getenv("LINK_TO_ANALYTIC_SERVICE")
	analyticsServiceURL := fmt.Sprintf("%s/event/append", linkToUserServer)

	eventData, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Printf("Error marshalling event: %v", err)
		return err
	}

	req, err := http.NewRequest("POST", analyticsServiceURL, bytes.NewBuffer(eventData))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Printf("Error creating request: %v", err)
		return err
	}

	req.Header.Set(contentType, appJson)
	otel.GetTextMapPropagator().Inject(context.Background(), propagation.HeaderCarrier(req.Header))
	client, err := createTLSClient()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Printf("Error creating TLS client: %v", err)
		return fmt.Errorf("failed to create TLS client: %v", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Printf("Error sending request to analytics service: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		span.RecordError(errors.New(eventFail))
		span.SetStatus(codes.Error, eventFail)
		log.Printf("Failed to send event to analytics service: %s", resp.Status)
		return fmt.Errorf("failed to send event to analytics service: %s", resp.Status)
	}
	span.SetStatus(codes.Ok, "Successfully sent event to analytics service")
	return nil
}

func (t *TasksHandler) publishStatusUpdate(ctx context.Context, task *model.Task) error {
	ctx, span := t.tracer.Start(ctx, "TaskHandler.publishStatusUpdate")
	defer span.End()
	nc, err := Conn()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}
	defer nc.Close()

	message := struct {
		TaskName   string   `json:"taskName"`
		TaskStatus string   `json:"taskStatus"`
		MemberIds  []string `json:"memberIds"`
	}{
		TaskName:   task.Name,
		TaskStatus: string(task.Status),
		MemberIds:  task.UserIDs,
	}

	jsonMessage, err := json.Marshal(message)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	subject := "task.status.update"
	err = nc.Publish(subject, jsonMessage)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to publish message to NATS: %w", err)
	}

	t.logger.Println("Task status update message sent to NATS")
	span.SetStatus(codes.Ok, "Successfully publish message to NATS")
	return nil
}

func (th *TasksHandler) HandleCheckingIfUserIsInTask(rw http.ResponseWriter, r *http.Request) {
	th.logger.Println("Received request to check if user is part of the task")
	th.custLogger.Info(nil, "Received request to check if user is part of the task")

	// Ekstrakcija zadatka iz konteksta
	_, span := th.tracer.Start(r.Context(), "TaskHandler.HandleCheckingIfUserIsInTask")
	defer span.End()
	task, ok := r.Context().Value(KeyTask{}).(*model.Task)
	if !ok || task == nil {
		span.RecordError(errors.New(taskData))
		span.SetStatus(codes.Error, taskData)
		http.Error(rw, taskData, http.StatusBadRequest)
		th.logger.Println("Error retrieving task from context")
		errMsg := taskData
		th.logger.Println(errMsg)
		th.custLogger.Error(nil, errMsg)
		http.Error(rw, errMsg, http.StatusBadRequest)
		return
	}
	th.custLogger.Info(logrus.Fields{"taskID": task.ID}, "Task extracted from context successfully")

	id, ok := r.Context().Value(KeyId{}).(string)
	if !ok || id == "" {
		span.RecordError(errors.New(missingId))
		span.SetStatus(codes.Error, missingId)
		http.Error(rw, missingId, http.StatusUnauthorized)
		th.logger.Println("Error retrieving user ID from context")
		errMsg := missingId
		th.logger.Println(errMsg)
		th.custLogger.Error(nil, errMsg)
		http.Error(rw, errMsg, http.StatusUnauthorized)
		return
	}
	th.custLogger.Info(logrus.Fields{"userID": id}, "User ID extracted from context successfully")

	itContains := slices.Contains(task.UserIDs, id)
	rw.Header().Set(contentType, "text/plain")

	if itContains {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("true"))
		th.logger.Println("User is part of the task")
		th.custLogger.Info(logrus.Fields{"taskID": task.ID, "userID": id}, "User is part of the task")
	} else {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("false"))
		th.logger.Println("User is not part of the task")
		th.custLogger.Info(logrus.Fields{"taskID": task.ID, "userID": id}, "User is not part of the task")
	}
	span.SetStatus(codes.Ok, "Successfully checked user")
}

func (h *TasksHandler) saveFileToHDFS(ctx context.Context, localFilePath, hdfsDirPath, hdfsFileName string) error {
	ctx, span := h.tracer.Start(ctx, "TaskHandler.SaveFileToHDFS")
	defer span.End()
	hdfsAddress := "namenode:9000"

	// Kreiranje HDFS klijenta
	hdfsClient, err := hdfs.New(hdfsAddress)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to initialize HDFS client: %w", err)
	}

	// Kreiranje direktorijuma u HDFS (ako ne postoji)
	err = hdfsClient.MkdirAll(hdfsDirPath, 0755)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to create directory in HDFS: %w", err)
	}

	// Putanja do fajla u HDFS
	hdfsFilePath := filepath.Join(hdfsDirPath, hdfsFileName)

	// Provera da li fajl postoji i brisanje ako postoji
	_, err = hdfsClient.Stat(hdfsFilePath)
	if err == nil {
		log.Printf("File exists in HDFS. Deleting: %s\n", hdfsFilePath)
		err = hdfsClient.Remove(hdfsFilePath)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("failed to delete existing file in HDFS: %w", err)
		}
	} else if !os.IsNotExist(err) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to check if file exists in HDFS: %w", err)
	}

	// Otvaranje lokalnog fajla za čitanje
	localFile, err := os.Open(localFilePath)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer localFile.Close()

	// Kreiranje fajla u HDFS
	hdfsFile, err := hdfsClient.Create(hdfsFilePath)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to create file in HDFS: %w", err)
	}
	defer hdfsFile.Close()

	// Kopiranje sadržaja iz lokalnog fajla u HDFS fajl
	_, err = io.Copy(hdfsFile, localFile)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("failed to write to HDFS file: %w", err)
	}

	log.Printf("File successfully written to HDFS: %s\n", hdfsFilePath)
	span.SetStatus(codes.Ok, "Successfully saved file to HDFS")
	return nil
}

func (h *TasksHandler) UploadTaskDocument(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.Start(r.Context(), "TaskHandler.UploadTaskDocument")
	defer span.End()
	// Parsiranje multipart forme
	err := r.ParseMultipartForm(10 << 20) // 10 MB
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Unable to parse form", http.StatusBadRequest)
		return
	}
	h.logger.Println("DEBUG::parsed form")

	// Preuzimanje fajla
	file, header, err := r.FormFile("file")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Unable to get file from form", http.StatusBadRequest)
		return
	}
	defer file.Close()
	h.logger.Println("DEBUG::Got file from form")

	// Preuzimanje taskId iz forme
	taskId := r.FormValue("taskId")
	if taskId == "" {
		span.RecordError(errors.New("missing taskId"))
		span.SetStatus(codes.Error, "missing taskId")
		http.Error(w, "Missing taskId in form data", http.StatusBadRequest)
		return
	}
	h.logger.Println("DEBUG::TaskId is found")

	// Kreiranje privremenog fajla na lokalnom disku
	tempDir := os.TempDir()
	tempFilePath := filepath.Join(tempDir, uuid.New().String()+"_"+header.Filename)
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Unable to create temporary file", http.StatusInternalServerError)
		return
	}
	defer tempFile.Close()

	h.logger.Println("DEBUG::Created temp file")

	// Kopiranje sadržaja iz primljenog fajla u privremeni fajl
	_, err = io.Copy(tempFile, file)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Unable to save file", http.StatusInternalServerError)
		return
	}
	h.logger.Println("DEBUG::Saved temp file")

	// Sačuvajte fajl u HDFS koristeći izdvojenu funkciju
	hdfsDirPath := "/example/"
	hdfsFileName := uuid.New().String() + "_" + header.Filename //header.Filename					//da ime bude bez uuid
	err = h.saveFileToHDFS(ctx, tempFilePath, hdfsDirPath, hdfsFileName)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, fmt.Sprintf("Failed to save file to HDFS: %v", err), http.StatusInternalServerError)
		return
	}

	h.logger.Println("DEBUG::File saved to HDFS")

	// Kreiranje TaskDocument objekta
	taskDocument := model.TaskDocument{
		ID:         primitive.NewObjectID(),
		TaskID:     taskId,
		FileName:   header.Filename,
		FileType:   header.Header.Get(contentType),
		FilePath:   filepath.Join(hdfsDirPath, hdfsFileName),
		UploadedAt: primitive.NewDateTimeFromTime(time.Now()),
	}

	err = h.documentRepo.SaveTaskDocument(ctx, &taskDocument)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Unable to save task document to database", http.StatusInternalServerError)
		return
	}

	id, ok := r.Context().Value(KeyId{}).(string)
	if !ok || id == "" {
		span.RecordError(errors.New(missingId))
		span.SetStatus(codes.Error, missingId)
		http.Error(w, missingId, http.StatusUnauthorized)
		h.logger.Println("Error retrieving user ID from context")
		errMsg := missingId
		h.logger.Println(errMsg)
		h.custLogger.Error(nil, errMsg)
		http.Error(w, errMsg, http.StatusUnauthorized)
		return
	}

	task, err := h.repo.GetByID(ctx, taskId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		h.logger.Print(dbErr, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	currentTime := time.Now().Add(1 * time.Hour)
	formattedTime := currentTime.Format(time.RFC3339)

	event := map[string]interface{}{
		"type": "DocumentAdded",
		"time": formattedTime,
		"event": map[string]interface{}{
			"taskId":     taskDocument.TaskID,
			"projectId":  task.ProjectID,
			"documentId": taskDocument.ID,
			"addedBy":    id,
		},
		"projectId": task.ProjectID,
	}

	if err := h.sendEventToAnalyticsService(ctx, event); err != nil {
		http.Error(w, eventFail, http.StatusInternalServerError)
		return
	}

	h.logger.Println("DEBUG::Saved task document to database")

	span.SetStatus(codes.Ok, "Successfully saved task document to HDFS")
	// Uspešan odgovor
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("File uploaded successfully"))
}

func (h *TasksHandler) GetTaskDocumentsByTaskID(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.Start(r.Context(), "TaskHandler.GetTaskDocumentsByTaskID")
	defer span.End()
	// Preuzimanje taskId iz query parametra
	vars := mux.Vars(r)
	taskID := vars["taskId"]

	h.logger.Printf("Fetching documents for taskId: %s", taskID)

	// Pozivanje metode iz repozitorijuma da se dobiju task dokumenti
	documents, err := h.documentRepo.GetTaskDocumentsByTaskID(ctx, taskID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		h.logger.Printf("Error fetching documents for taskId %s: %v", taskID, err)
		http.Error(w, "Unable to fetch task documents", http.StatusInternalServerError)
		return
	}

	// Vraćanje rezultata kao JSON
	w.Header().Set(contentType, appJson)
	err = json.NewEncoder(w).Encode(documents)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		h.logger.Printf("Error encoding documents for taskId %s: %v", taskID, err)
		http.Error(w, "Unable to encode task documents", http.StatusInternalServerError)
		return
	}
	span.SetStatus(codes.Ok, "Successfully fetched documents for taskId")
	h.logger.Printf("Successfully fetched %d documents for taskId %s", len(documents), taskID)
}

func (h *TasksHandler) getFileFromHDFS(ctx context.Context, hdfsFilePath string) ([]byte, error) {
	ctx, span := h.tracer.Start(ctx, "TaskHandler.GetFileFromHDFS")
	defer span.End()
	hdfsAddress := "namenode:9000"

	// Kreiranje HDFS klijenta
	hdfsClient, err := hdfs.New(hdfsAddress)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("failed to initialize HDFS client: %w", err)
	}

	// Otvaranje fajla iz HDFS-a
	hdfsFile, err := hdfsClient.Open(hdfsFilePath)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("failed to open HDFS file: %w", err)
	}
	defer hdfsFile.Close()

	// Čitanje sadržaja fajla
	fileContent, err := io.ReadAll(hdfsFile)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("failed to read HDFS file: %w", err)
	}
	span.SetStatus(codes.Ok, "Successfully read HDFS file")
	return fileContent, nil
}

func (h *TasksHandler) DownloadTaskDocument(w http.ResponseWriter, r *http.Request) {
	ctx, span := h.tracer.Start(r.Context(), "TaskHandler.DownloadTaskDocument")
	defer span.End()
	vars := mux.Vars(r)
	taskDocumentId, ok := vars["taskDocumentId"]
	if !ok || taskDocumentId == "" {
		span.RecordError(errors.New("missing taskDocumentId"))
		span.SetStatus(codes.Error, "missing taskDocumentId")
		http.Error(w, "Missing taskDocumentId in path parameters", http.StatusBadRequest)
		return
	}

	docID, err := primitive.ObjectIDFromHex(taskDocumentId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Invalid taskDocumentId format", http.StatusBadRequest)
		return
	}

	// Preuzimanje TaskDocument iz repozitorijuma
	taskDocument, err := h.documentRepo.GetTaskDocumentByID(ctx, docID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, fmt.Sprintf("Failed to fetch task document: %v", err), http.StatusInternalServerError)
		return
	}
	if taskDocument == nil {
		span.RecordError(errors.New("task document not found"))
		span.SetStatus(codes.Error, "task document not found")
		http.Error(w, "Task document not found", http.StatusNotFound)
		return
	}

	hdfsFilePath := taskDocument.FilePath

	// Preuzimanje fajla iz HDFS-a
	fileContent, err := h.getFileFromHDFS(ctx, hdfsFilePath)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, fmt.Sprintf("Failed to get file from HDFS: %v", err), http.StatusInternalServerError)
		return
	}

	// Postavljanje zaglavlja HTTP odgovora
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", hdfsFilePath))
	w.Header().Set(contentType, "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(fileContent)))

	// Slanje sadržaja fajla kao HTTP odgovor
	_, err = w.Write(fileContent)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Failed to send file content", http.StatusInternalServerError)
		return
	}
	span.SetStatus(codes.Ok, "Successfully sent file content")
	h.logger.Println("----------------------------------------------------------------------")
	h.logger.Println("DEBUG::File sent successfully:", hdfsFilePath)
	h.logger.Println("----------------------------------------------------------------------")

}

type TaskBlockedEvent struct {
	EventID string `json:"eventId"`
	TaskID  string `json:"taskId"`
	Blocked bool   `json:"blocked"`
	Reason  string `json:"reason"`
}

func (th *TasksHandler) BlockTask(rw http.ResponseWriter, r *http.Request) {
	ctx, span := th.tracer.Start(r.Context(), "TaskHandler.BlockTask")
	defer span.End()
	th.logger.Println("Received request to block task")
	th.custLogger.Info(nil, "Received request to block task")

	vars := mux.Vars(r)
	taskId := vars["taskId"]
	task, err := th.repo.GetByID(ctx, taskId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		th.logger.Print(dbErr, err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = th.repo.UpdateFlag(ctx, task, true)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		th.logger.Print(dbErr, err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	event := TaskBlockedEvent{
		TaskID:  task.ID.Hex(),
		Blocked: true,
		Reason:  "BlockTask",
	}
	eventData, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		th.logger.Print("Event serialization failed:", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := th.natsConn.Publish("TaskBlocked", eventData); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		th.logger.Print("Event publishing failed:", err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	th.logger.Println("Successfully blocked task and published event")

	span.SetStatus(codes.Ok, "Successfully blocked task")

}

func (th *TasksHandler) AddDependencyToTask(rw http.ResponseWriter, r *http.Request) {
	ctx, span := th.tracer.Start(r.Context(), "TaskHandler.AddDependencyToTask")
	defer span.End()

	th.logger.Println("Received request to add dependency to task")
	th.custLogger.Info(nil, "Received request to add dependency to task")

	vars := mux.Vars(r)
	taskId := vars["taskId"]
	task, err := th.repo.GetByID(ctx, taskId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		th.logger.Print(dbErr, err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	dependencyId := vars["dependencyId"]

	th.logger.Println("dependencyId: ", dependencyId)
	th.logger.Println("taskId: ", taskId)

	err = th.repo.AddDependency(ctx, task, dependencyId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		th.logger.Print(dbErr, err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	span.SetStatus(codes.Ok, "Successfully added dependency to task")

}

func (th *TasksHandler) listenForDependencyUpdates(ctx context.Context) {
	ctx, span := th.tracer.Start(ctx, "TaskHandler.listenForDependencyUpdates")
	defer span.End()
	nc, err := Conn()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		th.logger.Printf("Failed to connect to NATS: %v", err)
		return
	}
	defer nc.Close()

	_, err = nc.Subscribe("task.dependency.updated", func(msg *nats.Msg) {
		th.logger.Printf("Message received: %s", string(msg.Data))
		var event map[string]string
		err := json.Unmarshal(msg.Data, &event)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			th.logger.Printf("Error unmarshalling event: %v", err)
			return
		}

		taskID := event["taskID"]
		dependencyID := event["dependencyID"]

		th.logger.Printf("Processing dependency update for TaskID: %s, DependencyID: %s", taskID, dependencyID)
		th.updateBlockedStatus(ctx, dependencyID)
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Fatalf("Error subscribing to NATS subject: %v", err)
	}

	span.SetStatus(codes.Ok, "Successfully subscribed to NATS subject")

	select {}
}

func (th *TasksHandler) updateBlockedStatus(ctx context.Context, taskID string) {
	ctx, span := th.tracer.Start(ctx, "TaskHandler.UpdateBlockedStatus")
	defer span.End()
	th.custLogger.Info(nil, "Received nats request to change flag of task to blocked")
	th.logger.Println("Received nats request to change flag of task to blocked")

	task, err := th.repo.GetByID(ctx, taskID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		th.custLogger.Info(nil, "Cannot find task by id")
		return
	}
	task.Blocked = true

	//=======
	err = th.repo.UpdateFlag(ctx, task, true)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		th.custLogger.Info(nil, "Cannot find task by id")
		return
	}
	span.SetStatus(codes.Ok, "Successfully blocked task")
}

func (t *TasksHandler) RollbackTasks(ctx context.Context, projectID string) {

	ctx, span := t.tracer.Start(ctx, "TaskHandler.RollbackTasks")
	defer span.End()

	err := t.repo.UpdateAllTasksByProjectId(ctx, projectID, false)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		t.logger.Printf("Failed to rollbacked tasks for project %s: %v", projectID, err)

	}
	t.logger.Printf("Successfully rollbacked all tasks for project %s", projectID)

	span.SetStatus(codes.Ok, "Successfully rollbacked all tasks")

}
