package handlers

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"log"
	"net/http"
	"os"
	"project-service/client"
	"project-service/customLogger"
	"project-service/domain"
	"project-service/model"
	"project-service/repositories"
	"strconv"
	"strings"
	"time"
)

type KeyProject struct{}
type KeyUser struct{}
type KeyRole struct{}

const (
	appJson         = "application/json"
	contentType     = "Content-Type"
	databaseErr     = "Database exception: %s"
	jsonErr         = "Unable to convert to json "
	userCtxErr      = "User ID not found in context"
	invalidRole     = "Invalid role specified"
	dataErr         = "Database error"
	projectNotFound = "Project not found"
	retrievingErr   = "Error retrieving project"
	userMax         = "Cannot add more users than the maximum limit"
	userMin         = "Cannot add users to a project without meeting the minimum member requirement"
	natsErr         = "Error connecting to NATS:"
	roleNotFound    = "Role not found in context"
	successfulFunc  = "Successful function"
)

var pendingProjectDeletion = make(map[string]map[string]bool)

type ProjectsHandler struct {
	logger     *log.Logger
	custLogger *customLogger.Logger
	repo       *repositories.ProjectRepo
	tracer     trace.Tracer
	userClient client.UserClient
	taskClient client.TaskClient
}

type Task struct {
	Status  TaskStatus `bson:"status" json:"status"`
	UserIDs []string   `bson:"user_ids" json:"user_ids"`
}
type TaskStatus string

func (p *ProjectsHandler) MiddlewareExtractUserFromCookie(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, h *http.Request) {
		cookie, err := h.Cookie("auth_token")
		if err != nil {
			http.Error(rw, "No token found in cookie", http.StatusUnauthorized)
			p.logger.Println("No token in cookie:", err)
			return
		}

		userID, role, err := p.verifyTokenWithUserService(h.Context(), cookie.Value)
		if err != nil {
			http.Error(rw, "Invalid token", http.StatusUnauthorized)
			p.logger.Println("Invalid token:", err)
			return
		}

		ctx := context.WithValue(h.Context(), KeyUser{}, userID)
		ctx = context.WithValue(ctx, KeyRole{}, role)

		h = h.WithContext(ctx)

		next.ServeHTTP(rw, h)
	})
}

func ExtractTraceInfoMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (p *ProjectsHandler) verifyTokenWithUserService(ctx context.Context, token string) (string, string, error) {
	ctx, span := p.tracer.Start(ctx, "ProjectsHandler.verifyTokenWithUserService")
	defer span.End()

	userServiceUrl, err := p.getUserServiceURL()
	if err != nil {
		return p.recordSpanError(span, err)
	}

	req, err := p.createRequest(ctx, userServiceUrl, token, span)
	if err != nil {
		return p.recordSpanError(span, err)
	}

	cl, err := createTLSClient()
	if err != nil {
		return p.recordSpanError(span, err)
	}

	circuitBreaker := p.createCircuitBreaker()
	r := retrier.New(retrier.ConstantBackoff(3, 1000*time.Millisecond), retrier.WhitelistClassifier{domain.ErrRespTmp{}})
	retryCount := 0

	userID, role, err := p.executeWithRetry(ctx, cl, req, circuitBreaker, r, &retryCount, span)
	if err != nil {
		return "", "", err
	}

	return userID, role, nil
}

func (p *ProjectsHandler) recordSpanError(span trace.Span, err error) (string, string, error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	return "", "", err
}

func (p *ProjectsHandler) executeWithRetry(
	ctx context.Context,
	cl *http.Client,
	req *http.Request,
	cb *gobreaker.CircuitBreaker,
	r *retrier.Retrier,
	retryCount *int,
	span trace.Span,
) (string, string, error) {
	var userID, role string

	deadline, hasDeadline := ctx.Deadline()

	err := r.RunCtx(ctx, func(ctx context.Context) error {
		*retryCount++
		p.logger.Printf("Attempting validate-token request, attempt #%d", *retryCount)

		timeout := p.getTimeout(deadline, hasDeadline)
		if timeout > 0 {
			req.Header.Set("Timeout", strconv.Itoa(int(timeout.Milliseconds())))
		}

		_, err := cb.Execute(func() (interface{}, error) {
			return p.doRequest(cl, req, &userID, &role, span)
		})

		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	})

	return userID, role, err
}

func (p *ProjectsHandler) getTimeout(deadline time.Time, hasDeadline bool) time.Duration {
	if hasDeadline {
		return time.Until(deadline)
	}
	return 0
}

func (p *ProjectsHandler) doRequest(
	cl *http.Client,
	req *http.Request,
	userID *string,
	role *string,
	span trace.Span,
) (interface{}, error) {
	resp, err := cl.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
		return nil, domain.ErrRespTmp{
			URL:        resp.Request.URL.String(),
			Method:     resp.Request.Method,
			StatusCode: resp.StatusCode,
		}
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to validate token, status: %s", resp.Status)
	}

	var result struct {
		UserID string `json:"user_id"`
		Role   string `json:"role"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	*userID = result.UserID
	*role = result.Role
	return result, nil
}

func (p *ProjectsHandler) getUserServiceURL() (string, error) {
	linkToUserService := os.Getenv("LINK_TO_USER_SERVICE")
	return fmt.Sprintf("%s/validate-token", linkToUserService), nil
}

func (p *ProjectsHandler) createRequest(ctx context.Context, userServiceUrl,
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

func (p *ProjectsHandler) createCircuitBreaker() *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "UserServiceCircuitBreaker",
		MaxRequests: 5,
		Interval:    0,
		Timeout:     5 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures > 2
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			p.logger.Printf("Circuit Breaker '%s' changed from '%s' to '%s'", name, from, to)
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

func (p *ProjectsHandler) recordError(err error, message string, span trace.Span) {
	p.logger.Printf("%s : %v", message, err)
	span.RecordError(err)
	span.SetStatus(codes.Error, message)
}

func createTLSClient() (*http.Client, error) {
	caCert, err := os.ReadFile("/app/cert.crt")
	if err != nil {
		return nil, fmt.Errorf("failed to read certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append certs to the pool")
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

func NewProjectsHandler(l *log.Logger, custLogger *customLogger.Logger, r *repositories.ProjectRepo, tracer trace.Tracer, userClient client.UserClient, taskClient client.TaskClient) *ProjectsHandler {
	return &ProjectsHandler{logger: l, custLogger: custLogger, repo: r, tracer: tracer, userClient: userClient, taskClient: taskClient}
}

func (p *ProjectsHandler) GetAllProjects(rw http.ResponseWriter, h *http.Request) {
	ctx, span := p.tracer.Start(h.Context(), "ProjectsHandler.GetAllProjects")
	defer span.End()
	projects, err := p.repo.GetAll(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.logger.Print(fmt.Sprintf(databaseErr, err.Error()))
	}

	if projects == nil {
		return
	}

	err = projects.ToJSON(rw)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, jsonErr, http.StatusInternalServerError)
		p.logger.Fatal(jsonErr, err)
		return
	}
	span.SetStatus(codes.Ok, "Successfully got projects")
}

func (p *ProjectsHandler) GetAllProjectsByUser(rw http.ResponseWriter, h *http.Request) {
	ctx, span := p.tracer.Start(h.Context(), "ProjectsHandler.GetAllProjectsByUser")
	defer span.End()
	role, e := h.Context().Value(KeyRole{}).(string)
	if !e {
		span.RecordError(errors.New("No role found in context"))
		span.SetStatus(codes.Error, errors.New("No rule found").Error())
		http.Error(rw, "Role not defined", http.StatusBadRequest)
		p.logger.Println("Role not defined in context")
		p.custLogger.Warn(nil, "Role not defined in context")
		return
	}

	// Retrieve user ID from context
	userId, ok := h.Context().Value(KeyUser{}).(string)
	if !ok {
		span.RecordError(errors.New("No role found in context"))
		span.SetStatus(codes.Error, errors.New("No rule found").Error())
		http.Error(rw, "User ID not found", http.StatusUnauthorized)
		p.logger.Println(userCtxErr)
		p.custLogger.Warn(nil, userCtxErr)
		return
	}

	p.logger.Println("Processing request for user ID:", userId)
	p.custLogger.Info(logrus.Fields{
		"user_id": userId,
		"role":    role,
	}, "Processing request for user ID")

	var projects model.Projects
	var err error

	// Fetch projects based on the user's role
	if role == "manager" {
		p.logger.Println("Fetching projects for manager")
		p.custLogger.Info(logrus.Fields{
			"user_id": userId,
			"role":    role,
		}, "Fetching projects for manager")
		projects, err = p.repo.GetAllByManager(ctx, userId)
	} else if role == "member" {
		p.logger.Println("Fetching projects for member")
		p.custLogger.Info(logrus.Fields{
			"user_id": userId,
			"role":    role,
		}, "Fetching projects for member")
		projects, err = p.repo.GetAllByMember(ctx, userId)
	} else {
		span.RecordError(errors.New("There is an error"))
		span.SetStatus(codes.Error, errors.New("There is an error").Error())
		http.Error(rw, invalidRole, http.StatusBadRequest)
		p.logger.Println(invalidRole)
		p.custLogger.Warn(logrus.Fields{
			"role": role,
		}, invalidRole)
		return
	}

	// Handle database errors
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.logger.Print(fmt.Sprintf(databaseErr, err.Error()))
		p.logger.Print(fmt.Sprintf(databaseErr, err.Error()))
		p.custLogger.Error(logrus.Fields{
			"user_id": userId,
			"role":    role,
			"error":   err.Error(),
		}, databaseErr)
		http.Error(rw, dataErr, http.StatusInternalServerError)
		return
	}

	// Handle case where no projects are found := dont handle this because then when user wants to delete account, it cant because it can only delete it if hes not part of one in case of members
	//if projects == nil {
	//
	//	http.Error(rw, "No projects found", http.StatusNotFound)
	//	p.logger.Println("No projects found for user:", userId)
	//	p.custLogger.Warn(logrus.Fields{
	//		"user_id": userId,
	//		"role":    role,
	//	}, "No projects found for user")
	//	return
	//}

	// Convert projects to JSON and respond
	err = projects.ToJSON(rw)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, jsonErr, http.StatusInternalServerError)
		p.custLogger.Error(logrus.Fields{
			"user_id": userId,
			"role":    role,
			"error":   err.Error(),
		}, "Unable to convert projects to JSON")
		http.Error(rw, jsonErr, http.StatusInternalServerError)
		p.logger.Fatal("Unable to convert projects to JSON:", err)
		return
	}
	span.SetStatus(codes.Ok, "Successfully got projects")

	// Log success
	p.logger.Println("Successfully fetched projects for user:", userId)
	p.custLogger.Info(logrus.Fields{
		"user_id": userId,
		"role":    role,
	}, "Successfully fetched projects for user")
}

func (p *ProjectsHandler) GetProjectById(rw http.ResponseWriter, h *http.Request) {
	ctx, span := p.tracer.Start(h.Context(), "ProjectsHandler.GetProjectById")
	defer span.End()
	vars := mux.Vars(h)
	id := vars["id"]

	p.logger.Println("Fetching project with ID:", id)
	p.custLogger.Info(logrus.Fields{
		"project_id": id,
	}, "Fetching project by ID")

	// Fetch project from repository
	project, err := p.repo.GetById(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.logger.Print(fmt.Sprintf(databaseErr, err.Error()))
		p.logger.Printf(fmt.Sprintf(databaseErr, err.Error()))
		p.custLogger.Error(logrus.Fields{
			"project_id": id,
			"error":      err.Error(),
		}, "Database exception while fetching project by ID")
		http.Error(rw, dataErr, http.StatusInternalServerError)
		return
	}

	// Handle case where project is not found
	if project == nil {
		span.RecordError(errors.New("project is null"))
		span.SetStatus(codes.Error, "project is null")
		http.Error(rw, "Patient with given id not found", http.StatusNotFound)
		p.logger.Printf("Patient with id: '%s' not found", id)
		http.Error(rw, "Project with given ID not found", http.StatusNotFound)
		p.logger.Printf("Project with ID: '%s' not found", id)
		p.custLogger.Warn(logrus.Fields{
			"project_id": id,
		}, projectNotFound)
		return
	}

	// Convert project to JSON and respond
	err = project.ToJSON(rw)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())

		p.custLogger.Error(logrus.Fields{
			"project_id": id,
			"error":      err.Error(),
		}, "Unable to convert project to JSON")

		// Send HTTP response
		http.Error(rw, "Unable to convert project to JSON", http.StatusInternalServerError)
		return // Ensure no further execution
	}

	span.SetStatus(codes.Ok, "Successfully got project")

	// Log successful response
	p.logger.Printf("Successfully retrieved project with ID: '%s'", id)
	p.custLogger.Info(logrus.Fields{
		"project_id": id,
	}, "Successfully retrieved project")
}

func (p *ProjectsHandler) PostProject(rw http.ResponseWriter, h *http.Request) {
	ctx, span := p.tracer.Start(h.Context(), "ProjectsHandler.PostProject")
	defer span.End()
	// Retrieve project from context
	project, ok := h.Context().Value(KeyProject{}).(*model.Project)
	if !ok {
		http.Error(rw, "Invalid project data", http.StatusBadRequest)
		p.logger.Println(retrievingErr)
		p.custLogger.Warn(nil, retrievingErr)
		return
	}

	// Log received project details
	p.logger.Printf("Received project: %+v", project)
	p.custLogger.Info(logrus.Fields{
		"project_name": project.Name,
		"project_id":   project.ID,
	}, "Received project for insertion")

	// Insert project into repository
	id, err := p.repo.Insert(ctx, project)
	project.ID = id
	if err != nil {
		http.Error(rw, dataErr, http.StatusInternalServerError)
		p.logger.Printf("Error inserting project into database: %v", err)
		p.custLogger.Error(logrus.Fields{
			"project_name": project.Name,
			"project_id":   project.ID,
			"error":        err.Error(),
		}, "Database error while inserting project")
		return
	}

	currentTime := time.Now().Add(1 * time.Hour)
	formattedTime := currentTime.Format(time.RFC3339)

	event := map[string]interface{}{
		"type": "ProjectCreated",
		"time": formattedTime,
		"event": map[string]interface{}{
			"endDate": project.EndDate,
		},
		"projectId": project.ID,
	}

	if err := p.sendEventToAnalyticsService(ctx, event); err != nil {
		http.Error(rw, "Error sending event to analytics service", http.StatusInternalServerError)
		return
	}

	// Respond with success
	rw.WriteHeader(http.StatusCreated)
	span.SetStatus(codes.Ok, "Successfully created project")
	p.logger.Printf("Successfully inserted project with ID: %s", project.ID)
	p.custLogger.Info(logrus.Fields{
		"project_name": project.Name,
		"project_id":   project.ID,
	}, "Successfully create project")
}

func (p *ProjectsHandler) MiddlewareContentTypeSet(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, h *http.Request) {
		p.logger.Println("Method [", h.Method, "] - Hit path :", h.URL.Path)

		rw.Header().Add(contentType, appJson)

		next.ServeHTTP(rw, h)
	})
}

func (p *ProjectsHandler) MiddlewarePatientDeserialization(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, h *http.Request) {
		patient := &model.Project{}
		err := patient.FromJSON(h.Body)
		if err != nil {
			http.Error(rw, "Unable to decode json", http.StatusBadRequest)
			p.logger.Fatal(err)
			return
		}

		ctx := context.WithValue(h.Context(), KeyProject{}, patient)
		h = h.WithContext(ctx)

		next.ServeHTTP(rw, h)
	})
}
func (p *ProjectsHandler) AddUsersToProject(rw http.ResponseWriter, h *http.Request) {
	ctx, span := p.tracer.Start(h.Context(), "ProjectsHandler.AddUsersToProject")
	defer span.End()
	vars := mux.Vars(h)
	projectId := vars["id"]

	// Decode user IDs from request body
	var userIds []string
	err := json.NewDecoder(h.Body).Decode(&userIds)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, "Unable to decode JSON", http.StatusBadRequest)
		p.logger.Println("Error decoding JSON:", err)
		p.custLogger.Warn(logrus.Fields{
			"project_id": projectId,
			"error":      err.Error(),
		}, "Failed to decode JSON body for user IDs")
		return
	}

	// Retrieve project details
	project, err := p.repo.GetById(ctx, projectId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		http.Error(rw, retrievingErr, http.StatusInternalServerError)
		p.logger.Println(retrievingErr, err)
		p.custLogger.Error(logrus.Fields{
			"project_id": projectId,
			"error":      err.Error(),
		}, retrievingErr)
		return
	}

	// Retrieve user ID from context
	userId, ok := h.Context().Value(KeyUser{}).(string)
	if !ok {
		span.RecordError(errors.New("Unable to get user id"))
		span.SetStatus(codes.Error, "Unable to get user id")
		http.Error(rw, "User ID not found", http.StatusUnauthorized)
		p.logger.Println(userCtxErr)
		p.custLogger.Warn(logrus.Fields{
			"project_id": projectId,
		}, userCtxErr)
		return
	}

	// Check if the user is the project manager
	if project.Manager != userId {
		span.RecordError(errors.New("Project Manager does not match"))
		span.SetStatus(codes.Error, "Project Manager does not match")
		http.Error(rw, "Only the project manager can add users", http.StatusForbidden)
		p.logger.Printf("User %s is not the manager of project %s", userId, projectId)
		p.custLogger.Warn(logrus.Fields{
			"user_id":    userId,
			"project_id": projectId,
		}, "Unauthorized attempt to add users to project")
		return
	}

	// Check if the project has active tasks
	if !hasActiveTasksPlaceholder() {
		span.RecordError(errors.New("Does not have active tasks placeholder"))
		span.SetStatus(codes.Error, "Does not have active tasks placeholder")
		http.Error(rw, "Cannot add users to a project without active tasks", http.StatusForbidden)
		p.logger.Println("Project has no active tasks")
		p.custLogger.Warn(logrus.Fields{
			"project_id": projectId,
		}, "Attempt to add users to a project without active tasks")
		return
	}

	// Validate min and max members
	maxMembers, err := strconv.Atoi(project.MaxMembers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, "Invalid maximum members value", http.StatusInternalServerError)
		p.logger.Println("Invalid max members value:", err)
		p.custLogger.Error(logrus.Fields{
			"project_id": projectId,
			"error":      err.Error(),
		}, "Invalid max members value")
		return
	}

	minMembers, err := strconv.Atoi(project.MinMembers)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, "Invalid minimum members value", http.StatusInternalServerError)
		p.logger.Println("Invalid min members value:", err)
		p.custLogger.Error(logrus.Fields{
			"project_id": projectId,
			"error":      err.Error(),
		}, "Invalid min members value")
		return
	}

	currentMembersCount := len(userIds)
	if currentMembersCount > maxMembers {
		span.RecordError(errors.New(userMax))
		span.SetStatus(codes.Error, userMax)
		http.Error(rw, userMax, http.StatusForbidden)
		p.logger.Printf("Too many users for project %s: current=%d, max=%d", projectId, currentMembersCount, maxMembers)
		p.custLogger.Warn(logrus.Fields{
			"project_id":      projectId,
			"current_members": currentMembersCount,
			"max_members":     maxMembers,
		}, "Exceeded maximum members for project")
		return
	}

	if currentMembersCount < minMembers {
		span.RecordError(errors.New(userMin))
		span.SetStatus(codes.Error, userMin)
		http.Error(rw, userMin, http.StatusForbidden)
		p.logger.Printf("Too few users for project %s: current=%d, min=%d", projectId, currentMembersCount, minMembers)
		p.custLogger.Warn(logrus.Fields{
			"project_id":      projectId,
			"current_members": currentMembersCount,
			"min_members":     minMembers,
		}, "Below minimum members for project")
		return
	}

	// Add users to project
	err = p.repo.AddUsersToProject(ctx, projectId, userIds)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		http.Error(rw, "Error adding users to project", http.StatusInternalServerError)
		p.logger.Println("Error adding users to project:", err)
		p.custLogger.Error(logrus.Fields{
			"project_id": projectId,
			"error":      err.Error(),
		}, "Failed to add users to project")
		return
	}

	for _, uid := range userIds {
		subject := "project.joined"
		message := struct {
			UserID      string `json:"userId"`
			ProjectName string `json:"projectName"`
		}{
			UserID:      uid,
			ProjectName: project.Name,
		}

		if err := p.sendNotification(ctx, subject, message); err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}

		currentTime := time.Now().Add(1 * time.Hour)
		formattedTime := currentTime.Format(time.RFC3339)

		event := map[string]interface{}{
			"type": "MemberAdded",
			"time": formattedTime,
			"event": map[string]interface{}{
				"memberId":  uid,
				"projectId": projectId,
			},
			"projectId": projectId,
		}

		if err := p.sendEventToAnalyticsService(ctx, event); err != nil {
			http.Error(rw, "Error sending event to analytics service", http.StatusInternalServerError)
			return
		}
	}
	p.logger.Println("Messages sent to NATS for project:", projectId)

	p.custLogger.Info(logrus.Fields{
		"project_id": projectId,
		"user_ids":   userIds,
	}, "Messages sent to NATS for added users")

	// Respond with success
	rw.WriteHeader(http.StatusNoContent)
	span.SetStatus(codes.Ok, "Successfully added users to project")
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

func (p *ProjectsHandler) RemoveUserFromProject(rw http.ResponseWriter, h *http.Request) {
	ctx, span := p.tracer.Start(h.Context(), "ProjectsHandler.RemoveUserFromProject")
	defer span.End()
	vars := mux.Vars(h)
	projectId := vars["id"]
	userId := vars["userId"]

	project, err := p.repo.GetById(ctx, projectId)
	// Retrieve project details
	if err != nil {
		http.Error(rw, retrievingErr, http.StatusInternalServerError)
		p.logger.Printf("Error retrieving project with ID %s: %v", projectId, err)
		p.custLogger.Error(logrus.Fields{
			"project_id": projectId,
			"error":      err.Error(),
		}, retrievingErr)
		return
	}

	if project == nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, projectNotFound, http.StatusNotFound)
		p.logger.Printf("Project with ID %s not found", projectId)
		p.custLogger.Warn(logrus.Fields{
			"project_id": projectId,
		}, projectNotFound)
		return
	}

	// Retrieve auth token from cookie
	cookie, err := h.Cookie("auth_token")
	if err != nil {
		http.Error(rw, "Authentication token missing", http.StatusUnauthorized)
		p.logger.Println("Auth token missing in cookie:", err)
		p.custLogger.Warn(logrus.Fields{
			"project_id": projectId,
			"user_id":    userId,
			"error":      err.Error(),
		}, "Auth token missing")
		return
	}

	// Check if user is linked to active tasks
	if p.checkTasks(h.Context(), *project, userId, cookie) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, "User id added to active tasks, deletion blocked", http.StatusConflict)
		http.Error(rw, "User is linked to active tasks, removal blocked", http.StatusConflict)
		p.logger.Printf("User %s in project %s is linked to active tasks, removal blocked", userId, projectId)
		p.custLogger.Warn(logrus.Fields{
			"project_id": projectId,
			"user_id":    userId,
		}, "User is linked to active tasks, removal blocked")
		return
	}

	// Remove user from project
	err = p.repo.RemoveUserFromProject(ctx, projectId, userId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		http.Error(rw, "Error removing user from project", http.StatusInternalServerError)
		p.logger.Printf("Error removing user %s from project %s: %v", userId, projectId, err)
		p.custLogger.Error(logrus.Fields{
			"project_id": projectId,
			"user_id":    userId,
			"error":      err.Error(),
		}, "Failed to remove user from project")
		return
	}

	subject := "project.removed"
	message := struct {
		UserID      string `json:"userId"`
		ProjectName string `json:"projectName"`
	}{
		UserID:      userId,
		ProjectName: project.Name,
	}

	if err := p.sendNotification(ctx, subject, message); err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	currentTime := time.Now().Add(1 * time.Hour)
	formattedTime := currentTime.Format(time.RFC3339)

	event := map[string]interface{}{
		"type": "MemberRemoved",
		"time": formattedTime,
		"event": map[string]interface{}{
			"memberId":  userId,
			"projectId": projectId,
		},
		"projectId": projectId,
	}

	// Send the event to the analytic service
	if err := p.sendEventToAnalyticsService(ctx, event); err != nil {
		http.Error(rw, "Failed to send event to analytics service", http.StatusInternalServerError)
		return
	}

	p.logger.Printf("Removal message sent for user %s in project %s", userId, projectId)
	p.custLogger.Info(logrus.Fields{
		"project_id": projectId,
		"user_id":    userId,
	}, "Removal message sent to NATS")

	rw.WriteHeader(http.StatusOK)
	span.SetStatus(codes.Ok, "Successfully removed user from project")
}

func (p *ProjectsHandler) sendEventToAnalyticsService(ctx context.Context, event interface{}) error {
	ctx, span := p.tracer.Start(ctx, "ProjectsHandler.sendEventToAnalyticsService")
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
		span.RecordError(errors.New(resp.Status))
		span.SetStatus(codes.Error, resp.Status)
		log.Printf("Failed to send event to analytics service: %s", resp.Status)
		return fmt.Errorf("failed to send event to analytics service: %s", resp.Status)
	}
	span.SetStatus(codes.Ok, "Successfully sent event to analytics service")
	return nil
}

func (p *ProjectsHandler) DeleteProject(rw http.ResponseWriter, h *http.Request) {
	ctx, span := p.tracer.Start(h.Context(), "ProjectsHandler.DeleteProject")
	defer span.End()
	// Extract project ID from request
	vars := mux.Vars(h)
	projectId := vars["id"]

	project, err := p.repo.GetById(ctx, projectId)
	if err != nil {
		http.Error(rw, retrievingErr, http.StatusInternalServerError)
		p.logger.Printf("Error retrieving project with ID %s: %v", projectId, err)
		p.custLogger.Error(logrus.Fields{
			"project_id": projectId,
			"error":      err.Error(),
		}, retrievingErr)
		return
	}

	if project == nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, projectNotFound, http.StatusNotFound)
		p.logger.Printf("Project with ID %s not found", projectId)
		p.custLogger.Warn(logrus.Fields{
			"project_id": projectId,
		}, projectNotFound)
		return
	}

	// Log that project deletion has started
	p.logger.Printf("Deleting project with ID: %s", projectId)
	p.custLogger.Info(logrus.Fields{
		"project_id": projectId,
	}, "Deleting project")

	err = p.repo.PendingDeletion(ctx, projectId, true)
	//err = p.repo.DeleteProject(projectId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		http.Error(rw, "Error deleting project", http.StatusInternalServerError)
		p.logger.Printf("Error deleting project with ID %s: %v", projectId, err)
		p.custLogger.Error(logrus.Fields{
			"project_id": projectId,
			"error":      err.Error(),
		}, "Failed to delete project")
		return
	}

	nc, err := Conn()
	if err != nil {
		log.Println(natsErr, err)
		http.Error(rw, "Failed to connect to message broker", http.StatusInternalServerError)
		return
	}
	// publish a "ProjectDeleted" event to NATS, => this will be executed in taskHnadler/HandleProjectDeleted
	err = nc.Publish("ProjectDeleted", []byte(projectId))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.logger.Printf("Failed to publish ProjectDeleted event: %v", err)
		http.Error(rw, "Failed to publish event", http.StatusInternalServerError)
		p.logger.Printf("Failed to publish ProjectDeleted event for project ID %s: %v", projectId, err)
		p.custLogger.Error(logrus.Fields{
			"project_id": projectId,
			"error":      err.Error(),
		}, "Failed to publish ProjectDeleted event")
		return
	}

	p.logger.Printf("Project with ID %s successfully deleted", projectId)
	p.custLogger.Info(logrus.Fields{
		"project_id": projectId,
	}, "Project successfully deleted and event published")

	rw.WriteHeader(http.StatusOK)
	span.SetStatus(codes.Ok, "Successfully deleted project")
}

func (p *ProjectsHandler) SubscribeToEvent(ctx context.Context) {
	ctx, span := p.tracer.Start(ctx, "ProjectsHandler.SubscribeToEvent")
	defer span.End()
	nc, err := Conn()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Println(natsErr, err)
		p.logger.Printf(natsErr, err)

		return
	} else {
		p.logger.Printf("Success connecting to NATS!!!")
		log.Println("Success connecting to NATS!!!")

	}
	//defer nc.Close()

	_, err = nc.Subscribe("TasksDeleted", func(msg *nats.Msg) {
		projectID := string(msg.Data)
		if _, exists := pendingProjectDeletion[projectID]; !exists {
			pendingProjectDeletion[projectID] = make(map[string]bool)
		}
		pendingProjectDeletion[projectID]["TasksDeleted"] = true
		p.logger.Printf("Received TaskDeleted")

		if p.isDeletionReady(projectID) {
			p.HandleTasksDeleted(ctx, projectID)
			p.EmitSuccessMessage(projectID, nc)
		}

	})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.logger.Printf("Failed to subscribe to TasksDeleted event: %v", err)
	}
	_, err = nc.QueueSubscribe("TaskDeletionFailed", "task-failed-queue", func(msg *nats.Msg) {
		projectID := string(msg.Data)
		p.HandleTasksDeletedRollback(ctx, projectID)
	})
	_, err = nc.Subscribe("WorkflowsDeletionFailed", func(msg *nats.Msg) {
		projectID := string(msg.Data)
		p.HandleTasksDeletedRollback(ctx, projectID)
	})

	_, err = nc.QueueSubscribe("WorkflowsDeleted", "workflows-deleted-queue", func(msg *nats.Msg) {
		projectID := string(msg.Data)
		if _, exists := pendingProjectDeletion[projectID]; !exists {
			pendingProjectDeletion[projectID] = make(map[string]bool)
		}
		pendingProjectDeletion[projectID]["WorkflowsDeleted"] = true
		p.logger.Printf("Received WorkflowsDeleted")

		if p.isDeletionReady(projectID) {
			p.HandleTasksDeleted(ctx, projectID)
			p.EmitSuccessMessage(projectID, nc)
		}

	})
}

// emit message to finally phisically delete workflows and tasks
func (p *ProjectsHandler) EmitSuccessMessage(projectID string, nc *nats.Conn) {
	if nc == nil || nc.IsClosed() {
		p.logger.Println("NATS connection is nil or closed. Cannot emit success message.")
		return
	}

	if err := nc.Publish("TasksDeletionComplete", []byte(projectID)); err != nil {
		p.logger.Printf("Failed to publish TasksDeletionComplete for project %s: %v", projectID, err)
	}

	if err := nc.Publish("WorkflowsDeletionComplete", []byte(projectID)); err != nil {
		p.logger.Printf("Failed to publish WorkflowsDeletionComplete for project %s: %v", projectID, err)
	}

	p.logger.Printf("Successfully published success messages for project %s", projectID)
	//defer nc.Close() // comment this if not working
	//err = nc.Publish("TasksDeletionComplete", []byte(projectID))
	//if err != nil {
	//	p.logger.Printf("Failed to publish TasksDeletionComplete event for project %s: %v", projectID, err)
	//}
	//
	//err = nc.Publish("WorkflowsDeletionComplete", []byte(projectID))
	//if err != nil {
	//	p.logger.Printf("Failed to publish WorkflowsDeletionComplete event for project %s: %v", projectID, err)
	//}
	//
	//p.logger.Printf("Successfully published success messages for project %s", projectID)

}

func (p *ProjectsHandler) isDeletionReady(projectID string) bool {
	status, exists := pendingProjectDeletion[projectID]
	if !exists {
		return false
	}
	return status["TasksDeleted"] && status["WorkflowsDeleted"]
}

func (p *ProjectsHandler) HandleTasksDeleted(ctx context.Context, projectID string) {
	ctx, span := p.tracer.Start(ctx, "ProjectsHandler.HandleTasksDeleted")
	defer span.End()
	project, _ := p.repo.GetById(ctx, projectID)
	subject := "project.removed"
	for _, userID := range project.UserIDs {
		message := struct {
			UserID      string `json:"userId"`
			ProjectName string `json:"projectName"`
		}{
			UserID:      userID,
			ProjectName: project.Name,
		}

		if err := p.sendNotification(ctx, subject, message); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			p.logger.Println(err.Error())
			return
		}
	}
	p.logger.Println("a message has been sent")

	err := p.repo.DeleteProject(ctx, projectID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.logger.Printf("Failed to delete project %s: %v", projectID, err)
		return
	}

	span.SetStatus(codes.Ok, "Successfully deleted project")

	p.logger.Printf("Successfully deleted project %s", projectID)
}

func (p *ProjectsHandler) HandleTasksDeletedRollback(ctx context.Context, projectID string) {
	ctx, span := p.tracer.Start(ctx, "ProjectsHandler.HandleTasksDeletedRollback")
	defer span.End()
	err := p.repo.PendingDeletion(ctx, projectID, false)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.logger.Printf("Failed to delete project %s: %v", projectID, err)
		return
	}

	p.logger.Printf("Successfully retrive deleted project %s", projectID)
	span.SetStatus(codes.Ok, "Successfully retrive deleted project")
}

func (ph *ProjectsHandler) checkTasks(ctx context.Context, project model.Project, userID string, authTokenCookie *http.Cookie) bool {
	ctx, span := ph.tracer.Start(ctx, "ProjectsHandler.checkTasks")
	defer span.End()

	c, err := createTLSClient()
	if err != nil {
		return ph.handleError(span, "Error creating TLS client", err)
	}

	taskReq, err := ph.createTaskRequest(ctx, project.ID.Hex(), authTokenCookie)
	if err != nil {
		return ph.handleError(span, "Failed to create request to task-service", err)
	}

	circuitBreaker := ph.createCircuitBreakerForTaskService()
	retryAgain := ph.createRetrier()

	var taskResp *http.Response
	err = retryAgain.RunCtx(ctx, func(ctx context.Context) error {
		return ph.attemptTaskRequest(ctx, taskReq, c, circuitBreaker, &taskResp)
	})

	if err != nil {
		return ph.handleError(span, "Error during task-service request after retries", err)
	}

	defer taskResp.Body.Close()

	tasks, err := ph.decodeTaskResponse(taskResp)
	if err != nil {
		return ph.handleError(span, "Failed to decode task-service response", err)
	}

	return ph.checkUserTasks(tasks, userID, span)
}

func (ph *ProjectsHandler) createTaskRequest(ctx context.Context, projectID string, authTokenCookie *http.Cookie) (*http.Request, error) {
	taskServiceURL := fmt.Sprintf("https://task-server:8080/tasks/%s", projectID)
	taskReq, err := http.NewRequestWithContext(ctx, "GET", taskServiceURL, nil)
	if err != nil {
		return nil, err
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(taskReq.Header))
	taskReq.Header.Set(contentType, appJson)
	taskReq.AddCookie(authTokenCookie)
	return taskReq, nil
}

func (ph *ProjectsHandler) createCircuitBreakerForTaskService() *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "TaskServiceCircuitBreaker",
		MaxRequests: 5,
		Timeout:     5 * time.Second,
		Interval:    0,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures > 2
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			ph.logger.Printf("Circuit Breaker '%s' changed from '%s' to '%s'", name, from, to)
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

func (ph *ProjectsHandler) createRetrier() *retrier.Retrier {
	classifier := retrier.WhitelistClassifier{domain.ErrRespTmp{}}
	return retrier.New(retrier.ConstantBackoff(3, 1000*time.Millisecond), classifier)
}

func (ph *ProjectsHandler) attemptTaskRequest(ctx context.Context, taskReq *http.Request, client *http.Client, circuitBreaker *gobreaker.CircuitBreaker, resp **http.Response) error {
	_, err := circuitBreaker.Execute(func() (interface{}, error) {
		response, err := client.Do(taskReq)
		if err != nil {
			return nil, err
		}

		if response.StatusCode == http.StatusServiceUnavailable || response.StatusCode == http.StatusGatewayTimeout {
			return nil, domain.ErrRespTmp{
				URL:        response.Request.URL.String(),
				Method:     response.Request.Method,
				StatusCode: response.StatusCode,
			}
		}

		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("unexpected status code from task-service: %s", response.Status)
		}

		*resp = response
		return response, nil
	})

	return err
}

func (ph *ProjectsHandler) decodeTaskResponse(resp *http.Response) ([]Task, error) {
	var tasks []Task
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func (ph *ProjectsHandler) checkUserTasks(tasks []Task, userID string, span trace.Span) bool {
	for _, task := range tasks {
		if task.Status == "Pending" || task.Status == "InProgress" {
			for _, id := range task.UserIDs {
				if id == userID {
					span.SetStatus(codes.Ok, "User  is assigned to a task")
					return true
				}
			}
		}
	}
	span.SetStatus(codes.Ok, "No tasks found for the user")
	return false
}

func (ph *ProjectsHandler) handleError(span trace.Span, message string, err error) bool {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	ph.logger.Printf("%s: %v", message, err)
	return false
}

func hasActiveTasksPlaceholder() bool {
	return true
}

func (uh *ProjectsHandler) MiddlewareCheckRoles(allowedRoles []string, next http.Handler) http.Handler {

	return http.HandlerFunc(func(rw http.ResponseWriter, h *http.Request) {
		role, ok := h.Context().Value(KeyRole{}).(string)
		if !ok {
			http.Error(rw, "Forbidden", http.StatusForbidden)
			uh.logger.Println(roleNotFound)
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

func (p *ProjectsHandler) IsUserInProject(rw http.ResponseWriter, h *http.Request) {
	ctx, span := p.tracer.Start(h.Context(), "ProjectsHandler.IsUserInProject")
	defer span.End()
	vars := mux.Vars(h)
	projectID := vars["id"]
	userID := vars["userId"]

	isMember := p.isUserInProject(ctx, projectID, userID)

	if isMember {
		rw.WriteHeader(http.StatusOK)
		json.NewEncoder(rw).Encode(map[string]bool{"is_member": true})
	} else {
		rw.WriteHeader(http.StatusNotFound)
		json.NewEncoder(rw).Encode(map[string]bool{"is_member": false})
	}
	span.SetStatus(codes.Ok, successfulFunc)
}

func (p *ProjectsHandler) isUserInProject(ctx context.Context, projectID, userID string) bool {
	ctx, span := p.tracer.Start(ctx, "ProjectsHandler.isUserInProject")
	defer span.End()
	project, err := p.repo.GetById(ctx, projectID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.logger.Println("Error fetching project:", err)
		return false
	}

	span.SetStatus(codes.Ok, successfulFunc)
	return contains(project.UserIDs, userID)
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func (p *ProjectsHandler) CheckIfUserIsManager(rw http.ResponseWriter, h *http.Request) {
	ctx, span := p.tracer.Start(h.Context(), "ProjectsHandler.CheckIfUserIsManager")
	defer span.End()
	role, ok := h.Context().Value(KeyRole{}).(string)
	if !ok {
		span.RecordError(errors.New(roleNotFound))
		span.SetStatus(codes.Error, roleNotFound)
		http.Error(rw, roleNotFound, http.StatusUnauthorized)
		return
	}

	userId, ok := h.Context().Value(KeyUser{}).(string)
	if !ok {
		span.RecordError(errors.New("User not found in context"))
		span.SetStatus(codes.Error, "User not found in context")
		http.Error(rw, userCtxErr, http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(h)
	projectId := vars["id"]

	if role != "manager" {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("false"))
		return
	}

	isManager, err := p.repo.IsUserManagerOfProject(ctx, userId, projectId)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, "Error checking manager status", http.StatusInternalServerError)
		return
	}

	rw.Header().Set(contentType, appJson)
	rw.WriteHeader(http.StatusOK)
	if isManager {
		_, _ = rw.Write([]byte("true"))
	} else {
		_, _ = rw.Write([]byte("false"))
	}
	span.SetStatus(codes.Ok, successfulFunc)
}

func (p *ProjectsHandler) sendNotification(ctx context.Context, subject string, message interface{}) error {
	_, span := p.tracer.Start(ctx, "ProjectsHandler.AddUsersToProject")
	defer span.End()
	nc, err := Conn()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.Println(natsErr, err)
		p.logger.Println(natsErr, err)
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
func (p *ProjectsHandler) GetProjectDetailsById(rw http.ResponseWriter, h *http.Request) {
	ctx, span := p.tracer.Start(h.Context(), "ProjectsHandler.GetProjectDetailsById")
	defer span.End()
	// Step 1: Extract the auth_token cookie from the incoming request
	cookie, err := h.Cookie("auth_token")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, "No token found in cookie", http.StatusUnauthorized)
		p.logger.Println("No token in cookie:", err)
		return
	}

	// Step 2: Get the project ID from the URL
	vars := mux.Vars(h)
	id := vars["id"]

	// Step 3: Fetch the project from the database
	project, err := p.repo.GetById(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.logger.Print(fmt.Sprintf(databaseErr, err.Error()))
		http.Error(rw, "Error fetching project details", http.StatusInternalServerError)
		return
	}

	// If project is not found, return an error
	if project == nil {
		span.SetStatus(codes.Error, projectNotFound)
		span.SetStatus(codes.Error, projectNotFound)
		http.Error(rw, "Project with given id not found", http.StatusNotFound)
		p.logger.Printf("Project with id: '%s' not found", id)
		return
	}

	// Step 4: Fetch the user details associated with the project
	usersDetails, err := p.userClient.GetByIdsWithCookies(project.UserIDs, cookie)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, "Error fetching user details", http.StatusInternalServerError)
		return
	}

	// Step 5: Fetch the tasks associated with the project
	tasksDetails, err := p.taskClient.GetTasksByProjectId(id, cookie)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	// Step 6: Construct the response with project details, user details, and tasks
	projectDetails := client.ProjectDetails{
		ID:         project.ID,
		Name:       project.Name,
		EndDate:    project.EndDate,
		MinMembers: project.MinMembers,
		MaxMembers: project.MaxMembers,
		Users:      usersDetails,
		Tasks:      tasksDetails, // Add the task details to the response
		UserIDs:    project.UserIDs,
		Manager:    project.Manager,
	}

	// Step 7: Send the project details with users and tasks as a response
	err = projectDetails.ToJSON(rw)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, jsonErr, http.StatusInternalServerError)
		p.logger.Fatal(jsonErr, err)
		return
	}
	span.SetStatus(codes.Ok, "")
}
