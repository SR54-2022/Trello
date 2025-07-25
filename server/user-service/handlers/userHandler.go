package handlers

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"time"
	"user-service/customLogger"
	"user-service/data"
	"user-service/domain"
	"user-service/repository"
	"user-service/service"
)

type KeyAccount struct{}

type KeyRole struct{}

type UserHandler struct {
	logger     *log.Logger
	service    *service.UserService
	tracer     trace.Tracer
	custLogger *customLogger.Logger
}
type Task struct {
	Status  TaskStatus `bson:"status" json:"status"`
	UserIDs []string   `bson:"user_ids" json:"user_ids"`
}
type TaskStatus string

const (
	receivedReq     = "Received %s request for %s"
	contentType     = "Content-Type"
	appJson         = "application/json"
	responseErr     = "Error writing response: "
	jsonConvert     = "Unable to convert to json. "
	tokenValidation = "Token validation failed: "
	userNotFound    = "user id not found"
	encodeErr       = "Error encoding response: "
	reqBodyErr      = "Invalid request body"
	emailRequired   = "Email is required"
)

func NewUserHandler(logger *log.Logger, service *service.UserService, tracer trace.Tracer, custLogger *customLogger.Logger) *UserHandler {
	return &UserHandler{logger, service, tracer, custLogger}
}

func ExtractTraceInfoMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (uh *UserHandler) Registration(rw http.ResponseWriter, h *http.Request) {
	uh.logger.Printf(receivedReq, h.Method, h.URL.Path)
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.Registration")
	defer span.End()
	uh.custLogger.Info(nil, fmt.Sprintf(receivedReq, h.Method, h.URL.Path))

	// Decode request body
	request, err := decodeBody(h.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Printf("Error decoding request body: %v", err)
		uh.custLogger.Error(nil, "Error decoding request body: "+err.Error())
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	uh.custLogger.Info(logrus.Fields{"email": request.Email}, "Registration request body decoded successfully")

	// Process registration
	err = uh.service.Registration(ctx, request)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println("Registration error:", err)
		if errors.Is(err, data.ErrEmailAlreadyExists()) {
			uh.custLogger.Warn(logrus.Fields{"email": request.Email}, "Registration failed: Email already exists")
			http.Error(rw, `{"message": "Email already exists"}`, http.StatusConflict)
		} else {
			uh.custLogger.Error(logrus.Fields{"email": request.Email}, "Registration error: "+err.Error())
			http.Error(rw, `{"message": "`+err.Error()+`"}`, http.StatusInternalServerError)
		}
		return
	}
	uh.custLogger.Info(logrus.Fields{"email": request.Email}, "Registration successful")

	// Send success response
	rw.Header().Set(contentType, appJson)
	rw.WriteHeader(http.StatusCreated)
	response := map[string]string{"message": "Registration successful"}
	err = json.NewEncoder(rw).Encode(response)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(responseErr, err)
		uh.custLogger.Error(nil, "Error writing registration response: "+err.Error())
		return
	}
	span.SetStatus(codes.Ok, "Registration handled successfully")
	uh.custLogger.Info(nil, "Registration response sent successfully")
}

func (uh *UserHandler) GetManagers(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.GetManagers")
	defer span.End()
	managers, err := uh.service.GetAll(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Print("Database exception: ", err)
	}

	if managers == nil {
		return
	}

	err = managers.ToJSON(rw)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, jsonConvert, http.StatusInternalServerError)
		uh.logger.Fatal(jsonConvert, err)
		return
	}
	span.SetStatus(codes.Ok, "Retrieving managers handled successfully")
}

func (uh *UserHandler) MiddlewareExtractUserFromCookie(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, h *http.Request) {

		// Retrieve the auth token from the cookie
		cookie, err := h.Cookie("auth_token")
		if err != nil {
			http.Error(rw, "No token found in cookie", http.StatusUnauthorized)
			uh.logger.Println("No token in cookie:", err)
			uh.custLogger.Warn(logrus.Fields{
				"error": err.Error(),
			}, "No token found in cookie")
			return
		}

		uh.logger.Println("Token retrieved from cookie:", cookie.Value) // Log token value
		uh.custLogger.Info(logrus.Fields{
			"token": cookie.Value,
		}, "Token retrieved from cookie")

		// Validate the token
		userID, role, err := uh.service.ValidateToken(h.Context(), cookie.Value)
		if err != nil {
			uh.logger.Println(tokenValidation, err)
			uh.custLogger.Error(logrus.Fields{
				"token": cookie.Value,
				"error": err.Error(),
			}, "Token validation failed")
			http.Error(rw, `{"message": "Invalid token"}`, http.StatusUnauthorized)
			return
		}

		uh.logger.Println("Token validated successfully. User ID:", userID, "Role:", role)
		uh.custLogger.Info(logrus.Fields{
			"user_id": userID,
			"role":    role,
		}, "Token validated successfully mankiiiiiiiiiiii")

		// Add user ID and role to the request context
		ctx := context.WithValue(h.Context(), KeyAccount{}, userID)
		ctx = context.WithValue(ctx, KeyRole{}, role)

		// Update the request with the new context
		h = h.WithContext(ctx)

		// Call the next handler
		next.ServeHTTP(rw, h)
	})
}

func (uh *UserHandler) GetManager(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.GetManager")
	defer span.End()
	userID, ok := h.Context().Value(KeyAccount{}).(string)
	if !ok {
		span.RecordError(errors.New(userNotFound))
		span.SetStatus(codes.Error, errors.New(userNotFound).Error())
		http.Error(rw, userNotFound, http.StatusUnauthorized)
		uh.logger.Println(userNotFound)
		return
	}
	_, findOneSpan := uh.tracer.Start(context.Background(), "UserHandler.GetManager.FindOne")
	manager, err := uh.service.GetOne(ctx, userID)
	findOneSpan.End()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Print("Database exception: ", err)
		uh.logger.Print("Id: ", userID)
	}

	if manager == nil {
		span.RecordError(errors.New("manager not found"))
		span.SetStatus(codes.Error, errors.New("manager not found").Error())
		http.Error(rw, "Manager not found", http.StatusNotFound)
		return
	}

	err = manager.ToJSON(rw)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, jsonConvert, http.StatusInternalServerError)
		uh.logger.Fatal(jsonConvert, err)
		return
	}
	span.SetStatus(codes.Ok, "Manager retrieval handled successfully")
}

func (uh *UserHandler) DeleteUser(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.DeleteUser")
	defer span.End()

	userID, _ := h.Context().Value(KeyAccount{}).(string)

	manager, err := uh.getManager(ctx, userID, span, rw)
	if err != nil {
		return
	}

	clientToDo, err := createTLSClient()
	if err != nil {
		uh.logger.Printf("Error creating TLS client: %v", err)
		http.Error(rw, "Error creating TLS client", http.StatusInternalServerError)
		return
	}

	req, cookie, err := uh.buildProjectServiceRequest(ctx, h, rw, span)
	if err != nil {
		return
	}

	resp, err := uh.executeProjectServiceRequest(ctx, req, clientToDo)
	if err != nil {
		uh.logger.Println("Error during project service request:", err)
		http.Error(rw, "Error communicating with project service", http.StatusInternalServerError)
		return
	}

	if err := uh.handleProjectServiceResponse(ctx, rw, resp, userID, manager.Role, cookie, span); err != nil {
		return
	}

	if err := uh.service.Delete(ctx, userID); err != nil {
		uh.logger.Println("Failed to delete user:", err)
		http.Error(rw, "Error deleting user", http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusOK)
	rw.Write([]byte("User deleted successfully"))
	span.SetStatus(codes.Ok, "User deleted successfully")
}

func (uh *UserHandler) getManager(ctx context.Context, userID string, span trace.Span, rw http.ResponseWriter) (*data.Account, error) {
	manager, err := uh.service.GetOne(ctx, userID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Printf("Database exception: %v, Id: %s", err, userID)
		http.Error(rw, "Error fetching manager details", http.StatusInternalServerError)
		return nil, err
	}
	return manager, nil
}

func (uh *UserHandler) buildProjectServiceRequest(ctx context.Context, h *http.Request, rw http.ResponseWriter, span trace.Span) (*http.Request, *http.Cookie, error) {
	projectUrl := os.Getenv("LINK_TO_PROJECT_SERVICE")
	projectServiceURL := fmt.Sprintf("%s/projects", projectUrl)

	req, err := http.NewRequestWithContext(ctx, "GET", projectServiceURL, nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Printf("Failed to create request to project-service: %v", err)
		http.Error(rw, "Error communicating with project service", http.StatusInternalServerError)
		return nil, nil, err
	}

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	authTokenCookie, err := h.Cookie("auth_token")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println("No auth token cookie found:", err)
		http.Error(rw, "Authorization token required", http.StatusUnauthorized)
		return nil, nil, err
	}
	req.AddCookie(authTokenCookie)

	return req, authTokenCookie, nil
}

func (uh *UserHandler) executeProjectServiceRequest(ctx context.Context, req *http.Request, clientToDo *http.Client) (*http.Response, error) {
	projectBreaker := uh.createCircuitBreaker()
	retryAgain := retrier.New(retrier.ConstantBackoff(3, time.Second), retrier.WhitelistClassifier{domain.ErrRespTmp{}})
	var resp *http.Response

	err := retryAgain.RunCtx(ctx, func(ctx context.Context) error {
		_, err := projectBreaker.Execute(func() (interface{}, error) {
			var err error
			resp, err = clientToDo.Do(req)
			if err != nil {
				return nil, err
			}

			if resp.StatusCode == http.StatusGatewayTimeout || resp.StatusCode == http.StatusServiceUnavailable {
				return nil, domain.ErrRespTmp{
					URL:        resp.Request.URL.String(),
					Method:     resp.Request.Method,
					StatusCode: resp.StatusCode,
				}
			}

			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
				return nil, domain.ErrResp{
					URL:        resp.Request.URL.String(),
					Method:     resp.Request.Method,
					StatusCode: resp.StatusCode,
				}
			}

			return resp, nil
		})
		return err
	})

	return resp, err
}

func (uh *UserHandler) handleProjectServiceResponse(
	ctx context.Context,
	rw http.ResponseWriter,
	resp *http.Response,
	userID, role string,
	authCookie *http.Cookie,
	span trace.Span,
) error {
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
		var projects []service.Project
		if resp.StatusCode == http.StatusOK {
			if err := json.NewDecoder(resp.Body).Decode(&projects); err != nil {
				uh.logger.Println("Failed to decode project-service response:", err)
				http.Error(rw, "Error parsing project service response", http.StatusInternalServerError)
				return err
			}
		}

		if role == "member" && uh.hasActiveProjects(projects, userID) {
			http.Error(rw, "Member has active projects, deletion blocked", http.StatusConflict)
			return errors.New("member has active projects")
		}

		if uh.checkTasks(ctx, projects, userID, role, authCookie) {
			http.Error(rw, "User has active projects, deletion blocked", http.StatusConflict)
			return errors.New("user has active projects")
		}

		return nil
	}

	uh.logger.Printf("Unexpected response code %d from project service\n", resp.StatusCode)
	http.Error(rw, "Error checking manager projects", http.StatusInternalServerError)
	span.RecordError(errors.New("unexpected project service response"))
	return errors.New("unexpected response from project service")
}

func (uh *UserHandler) createCircuitBreaker() *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(
		gobreaker.Settings{
			Name:        "DeleteUserProjectService",
			MaxRequests: 10,
			Timeout:     10 * time.Second,
			Interval:    0,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				return counts.ConsecutiveFailures > 2
			},
			OnStateChange: func(name string, from, to gobreaker.State) {
				uh.logger.Printf("Circuit Breaker '%s' changed from '%s' to '%s'\n", name, from, to)
			},
		},
	)
}

func (uh *UserHandler) hasActiveProjects(projects []service.Project, userID string) bool {
	for _, project := range projects {
		for _, user := range project.UserIDs {
			if user == userID {
				return true
			}
		}
	}
	return false
}

func (uh *UserHandler) handleError(span trace.Span, err error, rw http.ResponseWriter, message string, statusCode int) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	uh.logger.Println(message, ":", err)
	http.Error(rw, message, statusCode)
}

func (uh *UserHandler) checkTasks(ctx context.Context, projects []service.Project, userID, role string, authTokenCookie *http.Cookie) bool {
	ctx, span := uh.tracer.Start(ctx, "User Handler.checkTasks")
	defer span.End()

	tlsClient, err := createTLSClient()
	if err != nil {
		uh.recordError(span, err, "Error creating TLS client")
		return false
	}

	taskServiceBreaker := uh.createCircuitBreaker()

	retryAgain := retrier.New(retrier.ConstantBackoff(3, 1000*time.Millisecond), retrier.WhitelistClassifier{domain.ErrRespTmp{}})

	for _, project := range projects {
		if uh.checkProjectTasks(ctx, project, userID, role, authTokenCookie, tlsClient, taskServiceBreaker, retryAgain, span) {
			return true
		}
	}

	span.SetStatus(codes.Ok, "No active tasks found")
	return false
}

func (uh *UserHandler) checkProjectTasks(ctx context.Context, project service.Project, userID, role string, authTokenCookie *http.Cookie, tlsClient *http.Client, taskServiceBreaker *gobreaker.CircuitBreaker, retryAgain *retrier.Retrier, span trace.Span) bool {
	taskServiceURL := fmt.Sprintf("%s/tasks/%s", os.Getenv("LINK_TO_TASK_SERVICE"), project.ID)
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	taskReq, err := uh.createTaskRequest(reqCtx, taskServiceURL, authTokenCookie)
	if err != nil {
		uh.recordError(span, err, "Failed to create request to task-service")
		return false
	}

	taskResp, err := uh.sendTaskServiceRequest(reqCtx, taskReq, tlsClient, taskServiceBreaker, retryAgain, span)
	if err != nil {
		uh.logger.Printf("Error during task service request for project %s: %v", project.ID, err)
		return false
	}
	defer taskResp.Body.Close()

	if taskResp.StatusCode == http.StatusServiceUnavailable || taskResp.StatusCode == http.StatusGatewayTimeout {
		uh.logger.Printf("Temporary error (503/504) from task-service for project %s, terminating user check", project.ID)
		return true
	}

	if taskResp.StatusCode != http.StatusOK && taskResp.StatusCode != http.StatusNoContent {
		uh.logger.Printf("Unexpected response from task-service for project %s: %s", project.ID, taskResp.Status)
		return false
	}

	return uh.processTaskResponse(taskResp, userID, role, span)
}

func (uh *UserHandler) createTaskRequest(ctx context.Context, taskServiceURL string, authTokenCookie *http.Cookie) (*http.Request, error) {
	taskReq, err := http.NewRequestWithContext(ctx, "GET", taskServiceURL, nil)
	if err != nil {
		return nil, err
	}

	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(taskReq.Header))
	taskReq.Header.Set(contentType, appJson)
	taskReq.AddCookie(authTokenCookie)

	return taskReq, nil
}

func (uh *UserHandler) sendTaskServiceRequest(ctx context.Context, taskReq *http.Request, tlsClient *http.Client, taskServiceBreaker *gobreaker.CircuitBreaker, retryAgain *retrier.Retrier, span trace.Span) (*http.Response, error) {
	var taskResp *http.Response

	err := retryAgain.RunCtx(ctx, func(ctx context.Context) error {
		_, err := taskServiceBreaker.Execute(func() (interface{}, error) {
			resp, err := tlsClient.Do(taskReq)
			if err != nil {
				return nil, err
			}

			if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout {
				return nil, domain.ErrRespTmp{
					URL:        resp.Request.URL.String(),
					Method:     resp.Request.Method,
					StatusCode: resp.StatusCode,
				}
			}

			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
				return nil, domain.ErrResp{
					URL:        resp.Request.URL.String(),
					Method:     resp.Request.Method,
					StatusCode: resp.StatusCode,
				}
			}

			taskResp = resp
			return resp, nil
		})

		return err
	})

	return taskResp, err
}

func (uh *UserHandler) processTaskResponse(taskResp *http.Response, userID, role string, span trace.Span) bool {
	var tasks []Task
	if err := json.NewDecoder(taskResp.Body).Decode(&tasks); err != nil {
		uh.recordError(span, err, "Failed to decode task-service response")
		return false
	}

	if role == "manager" {
		return uh.hasActiveTasksForManager(tasks, span)
	}
	return uh.hasActiveTasksForMember(tasks, userID, span)
}

func (uh *UserHandler) hasActiveTasksForManager(tasks []Task, span trace.Span) bool {
	for _, task := range tasks {
		if task.Status == "Pending" || task.Status == "InProgress" {
			span.SetStatus(codes.Ok, "Tasks found for manager")
			return true
		}
	}
	return false
}

func (uh *UserHandler) hasActiveTasksForMember(tasks []Task, userID string, span trace.Span) bool {
	for _, task := range tasks {
		if task.Status == "Pending" || task.Status == "InProgress" {
			for _, id := range task.UserIDs {
				if id == userID {
					span.SetStatus(codes.Ok, "Task found for member")
					return true
				}
			}
		}
	}
	return false
}

func (uh *UserHandler) recordError(span trace.Span, err error, message string) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	uh.logger.Printf("%s: %v", message, err)
}

func decodeBody(r io.Reader) (*data.AccountRequest, error) {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()

	var c data.AccountRequest
	if err := dec.Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

func decodeLoginBody(r io.Reader) (*data.LoginCredentials, error) {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()

	var c data.LoginCredentials
	if err := dec.Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (uh *UserHandler) GetAllMembers(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.GetAllMembers")
	defer span.End()
	uh.logger.Printf(receivedReq, h.Method, h.URL.Path)

	accounts, err := uh.service.GetAllMembers(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println("Error retrieving members:", err)
		http.Error(rw, `{"message": "`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	rw.Header().Set(contentType, appJson)
	rw.WriteHeader(http.StatusOK)
	err = json.NewEncoder(rw).Encode(accounts)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(responseErr, err)
		return
	}
	span.SetStatus(codes.Ok, "Successfully retrieved members")
}

func (uh *UserHandler) VerifyTokenExistence(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.VerifyTokenExistence")
	defer span.End()
	userID := h.Header.Get("X-User-ID")
	if userID == "" {
		span.RecordError(errors.New("user id is missing"))
		span.SetStatus(codes.Error, "user id is missing")
		http.Error(rw, "User ID missing", http.StatusBadRequest)
		return
	}
	repo, _ := repository.New(ctx, uh.logger, uh.custLogger, uh.tracer)

	cache, err := repository.NewCache(uh.logger, repo, uh.tracer, uh.custLogger)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println("Error initializing cache:", err)
		http.Error(rw, `{"message": "Internal Server Error"}`, http.StatusInternalServerError)
		return
	}

	exists, err := cache.VerifyToken(ctx, userID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println("Error verifying token:", err)
		http.Error(rw, `{"message": "`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	if exists {
		rw.Write([]byte("true"))
	} else {
		rw.Write([]byte("false"))
	}
	span.SetStatus(codes.Ok, "Verification handled successfully")
}

func (uh *UserHandler) Login(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.Login")
	defer span.End()
	uh.logger.Println("Processing login request")
	uh.custLogger.Info(nil, "Processing login request")

	// Decode the login request body
	request, err := decodeLoginBody(h.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println("Error decoding request:", err)
		uh.custLogger.Error(nil, "Error decoding request: "+err.Error())
		http.Error(rw, err.Error(), http.StatusBadRequest)
		return
	}
	uh.custLogger.Info(logrus.Fields{}, "Login request decoded successfully")

	//emailRegex := `^[a-zA-Z0-9]{2,}\.[a-zA-Z0-9]{2,}@[a-zA-Z0-9]+\.[a-zA-Z]{2,}$`
	emailRegex := `^.+@.+$`

	matched, err := regexp.MatchString(emailRegex, request.Email)
	if err != nil || !matched {
		span.RecordError(fmt.Errorf("invalid email format"))
		span.SetStatus(codes.Error, "invalid email format")
		uh.logger.Println("Invalid email format:", request.Email)
		http.Error(rw, `{"message": "Invalid email format. Use 'ime.prezime@gmail.com'"}`, http.StatusBadRequest)
		return
	}

	// Verify reCAPTCHA
	boolean, err := uh.service.VerifyRecaptcha(ctx, request.RecaptchaToken)
	if !boolean {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			uh.logger.Println("Error verifying reCAPTCHA:", err)
			uh.custLogger.Error(nil, "Error verifying reCAPTCHA: "+err.Error())
			http.Error(rw, err.Error(), http.StatusForbidden)
			return
		}
		uh.logger.Println("reCAPTCHA validation failed")
		uh.custLogger.Warn(nil, "reCAPTCHA validation failed")
		http.Error(rw, "Error validating reCAPTCHA", http.StatusForbidden)
		return
	}
	uh.custLogger.Info(nil, "reCAPTCHA verified successfully")

	// Process login
	id, role, token, err := uh.service.Login(ctx, request)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println("Error logging in:", err)
		uh.custLogger.Error(logrus.Fields{
			"user_email": request.Email,
		}, "Error logging in: "+err.Error())
		http.Error(rw, `{"message": "`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	uh.custLogger.Info(logrus.Fields{
		"user_id": id,
		"role":    role,
	}, "Login successful")

	// Set auth token cookie
	http.SetCookie(rw, &http.Cookie{
		Name:     "auth_token",
		Value:    token,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode, // Set SameSite policy to prevent CSRF attacks
		Path:     "/",                     // Cookie valid for the entire site
	})
	uh.custLogger.Info(logrus.Fields{
		"token": "auth_token_set",
	}, "Authentication token set in cookie")

	// Send success response
	rw.Header().Set(contentType, appJson)
	rw.WriteHeader(http.StatusCreated)

	response := map[string]string{
		"id":   id,
		"role": role,
	}
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(encodeErr, err)
		uh.custLogger.Error(nil, encodeErr+err.Error())
		http.Error(rw, `{"message": "Internal Server Error"}`, http.StatusInternalServerError)
		return
	}

	_, err = rw.Write(jsonResponse)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(responseErr, err)
		uh.custLogger.Error(nil, responseErr+err.Error())
	}
	span.SetStatus(codes.Ok, "Successfully logged in")
	uh.logger.Println("Login response sent successfully")
	uh.custLogger.Info(nil, "Login response sent successfully")
}

func (uh *UserHandler) Logout(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.Logout")
	defer span.End()
	// Dohvatanje User ID iz konteksta
	userID, ok := h.Context().Value(KeyAccount{}).(string)
	if !ok {
		span.RecordError(errors.New(userNotFound))
		span.SetStatus(codes.Error, userNotFound)
		http.Error(rw, userNotFound, http.StatusUnauthorized)
		uh.logger.Println(userNotFound)
		uh.custLogger.Warn(nil, userNotFound)
		return
	}
	uh.custLogger.Info(logrus.Fields{"user_id": userID}, "Processing logout request")

	// Poziv servisa za logout
	err := uh.service.Logout(ctx, userID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println("Error logging out:", err)
		uh.custLogger.Error(logrus.Fields{"user_id": userID}, "Error logging out: "+err.Error())
		http.Error(rw, `{"message": "Internal Server Error"}`, http.StatusInternalServerError)
		return
	}
	uh.custLogger.Info(logrus.Fields{"user_id": userID}, "User logged out successfully")

	// Brisanje auth tokena
	http.SetCookie(rw, &http.Cookie{
		Name:     "auth_token",
		Value:    "", // Brisanje vrednostiiiii
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
		Path:     "/", // Cookie važi za cijeliiiii
		MaxAge:   0,   // MaxAge 0 za brisanje cookie-jaaaaaa
	})
	uh.custLogger.Info(logrus.Fields{"user_id": userID}, "Authentication token cleared in cookie")

	// Slanje uspešnog odgovora
	rw.Header().Set(contentType, appJson)
	rw.WriteHeader(http.StatusOK)

	response := map[string]string{"message": "Logged out successfully"}
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(encodeErr, err)
		uh.custLogger.Error(logrus.Fields{"user_id": userID}, "Error encoding logout response: "+err.Error())
		http.Error(rw, `{"message": "Internal Server Error"}`, http.StatusInternalServerError)
		return
	}

	_, err = rw.Write(jsonResponse)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(responseErr, err)
		uh.custLogger.Error(logrus.Fields{"user_id": userID}, "Error writing logout response: "+err.Error())
	}
	span.SetStatus(codes.Ok, "Successfully logged out")
	uh.logger.Println("Logout response sent successfully")
	uh.custLogger.Info(logrus.Fields{"user_id": userID}, "Logout response sent successfully")
}

func (uh *UserHandler) CheckPasswords(rw http.ResponseWriter, r *http.Request) {
	ctx, span := uh.tracer.Start(r.Context(), "UserHandler.CheckPasswords")
	defer span.End()
	var req data.ChangePasswordRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, reqBodyErr, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	userID, ok := r.Context().Value(KeyAccount{}).(string)
	if !ok {
		span.RecordError(errors.New(userNotFound))
		span.SetStatus(codes.Error, userNotFound)
		http.Error(rw, userNotFound, http.StatusUnauthorized)
		uh.logger.Println(userNotFound)
		return
	}

	isPasswordCorrect := uh.service.PasswordCheck(ctx, userID, req.Password)
	uh.logger.Println("Password is correct:", isPasswordCorrect)

	responseString := "false"
	if isPasswordCorrect {
		responseString = "true"
	}

	rw.Header().Set(contentType, "text/plain")
	rw.WriteHeader(http.StatusOK)

	_, writeErr := rw.Write([]byte(responseString))
	if writeErr != nil {
		span.RecordError(writeErr)
		span.SetStatus(codes.Error, writeErr.Error())
		http.Error(rw, "Failed to write response", http.StatusInternalServerError)
		return
	}
	span.SetStatus(codes.Ok, "Successfully checked passwords")
}

func (uh *UserHandler) ChangePassword(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.ChangePassword")
	defer span.End()
	uh.custLogger.Info(nil, "Processing change password request")

	// Parsiranje zahteva
	var req data.ChangePasswordRequest
	err := json.NewDecoder(h.Body).Decode(&req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, reqBodyErr, http.StatusBadRequest)
		uh.logger.Println(reqBodyErr, err)
		uh.custLogger.Error(nil, reqBodyErr+err.Error())
		return
	}
	defer h.Body.Close()
	uh.custLogger.Info(nil, "Request body decoded successfully")

	// Dohvatanje korisničkog ID-a iz konteksta
	userID, ok := h.Context().Value(KeyAccount{}).(string)
	if !ok {
		span.RecordError(errors.New(userNotFound))
		span.SetStatus(codes.Error, userNotFound)
		http.Error(rw, userNotFound, http.StatusUnauthorized)
		uh.logger.Println(userNotFound)
		uh.custLogger.Warn(nil, userNotFound)
		return
	}
	uh.custLogger.Info(logrus.Fields{"user_id": userID}, "User ID retrieved from context")

	// Promena lozinke
	err = uh.service.ChangePassword(ctx, userID, req.Password)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, "Failed to change password", http.StatusInternalServerError)
		uh.logger.Println("Failed to change password:", err)
		uh.custLogger.Error(logrus.Fields{"user_id": userID}, "Failed to change password: "+err.Error())
		return
	}
	uh.custLogger.Info(logrus.Fields{"user_id": userID}, "Password changed successfully")

	// Uspešan odgovor
	rw.WriteHeader(http.StatusOK)
	span.SetStatus(codes.Ok, "Successfully changed password")
	uh.custLogger.Info(logrus.Fields{"user_id": userID}, "Change password response sent successfully")
}

func (uh *UserHandler) HandleRecovery(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.HandleRecovery")
	defer span.End()
	var req struct {
		Email string `json:"email"`
	}

	err := json.NewDecoder(h.Body).Decode(&req)
	if err != nil || req.Email == "" {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, "Invalid request body or missing email", http.StatusBadRequest)
		return
	}
	defer h.Body.Close()

	err = uh.service.RecoveryRequest(ctx, req.Email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusOK)
	span.SetStatus(codes.Ok, "Successfully handled recovery request")
}

func (uh *UserHandler) HandlePasswordReset(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.HandlePasswordReset")
	defer span.End()
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	err := json.NewDecoder(h.Body).Decode(&req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, reqBodyErr, http.StatusBadRequest)
		return
	}
	defer h.Body.Close()
	err = uh.service.ResettingPassword(ctx, req.Email, req.Password)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
	span.SetStatus(codes.Ok, "Successfully handled password reset request")
}

func (uh *UserHandler) HandleMagic(rw http.ResponseWriter, r *http.Request) {
	ctx, span := uh.tracer.Start(r.Context(), "UserHandler.HandleMagic")
	defer span.End()
	var req struct {
		Email string `json:"email"`
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, reqBodyErr, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	err = uh.service.MagicLink(ctx, req.Email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
	span.SetStatus(codes.Ok, "Successfully handled magic request")
}

func (uh *UserHandler) HandleMagicVerification(rw http.ResponseWriter, r *http.Request) {
	ctx, span := uh.tracer.Start(r.Context(), "UserHandler.HandleMagicVerification")
	defer span.End()
	var req struct {
		Email string `json:"email"`
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, reqBodyErr, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	id, token, err := uh.service.VerifyMagic(ctx, req.Email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	http.SetCookie(rw, &http.Cookie{
		Name:     "auth_token",
		Value:    token,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode, // Prevents CSRF attacks
		Path:     "/",
	})

	rw.WriteHeader(http.StatusOK)
	rw.Header().Set(contentType, appJson)

	jsonResponse, err := json.Marshal(id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(encodeErr, err)
		http.Error(rw, `{"message": "Internal Server Error"}`, http.StatusInternalServerError)
		return
	}
	_, err = rw.Write(jsonResponse)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(responseErr, err)
		http.Error(rw, `{"message": "Internal Server Error"}`, http.StatusInternalServerError)
		return
	}
	span.SetStatus(codes.Ok, "Successfully logged in")

}

func (uh *UserHandler) ValidateToken(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.ValidateToken")
	defer span.End()
	uh.logger.Println("The path is hit")
	var req struct {
		Token string `json:"token"`
	}
	err := json.NewDecoder(h.Body).Decode(&req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println("Error decoding request body:", err)
		http.Error(rw, reqBodyErr, http.StatusBadRequest)
		return
	}
	defer h.Body.Close()

	userID, role, err := uh.service.ValidateToken(ctx, req.Token)
	uh.logger.Println("User ID is:", userID, "Role is:", role)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(tokenValidation, err)
		http.Error(rw, `{"message": "Invalid token"}`, http.StatusUnauthorized)
		return
	}

	response := map[string]string{
		"user_id": userID,
		"role":    role,
	}
	rw.Header().Set(contentType, appJson)
	rw.WriteHeader(http.StatusOK)
	err = json.NewEncoder(rw).Encode(response)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(responseErr, err)
		http.Error(rw, "Internal Server Error", http.StatusInternalServerError)
	}
	span.SetStatus(codes.Ok, "Successfully validated token")
}

func (uh *UserHandler) MiddlewareCheckRoles(allowedRoles []string, next http.Handler) http.Handler {
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

func (uh *UserHandler) MiddlewareCheckAuthenticated(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {

		cookie, err := r.Cookie("auth_token")
		if err == nil && cookie != nil {
			_, _, err := uh.service.ValidateToken(r.Context(), cookie.Value)
			if err == nil {
				http.Error(rw, "You are already logged in", http.StatusForbidden)
				uh.logger.Println("User is already authenticated. Forbidden access.")
				return
			}
		}

		next.ServeHTTP(rw, r)
	})
}

func (uh *UserHandler) GetUsersByIds(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.GetUsersByIds")
	defer span.End()
	uh.logger.Printf(receivedReq, h.Method, h.URL.Path)

	var request data.UserIdsRequest
	err := json.NewDecoder(h.Body).Decode(&request)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, reqBodyErr, http.StatusBadRequest)
		uh.logger.Println("Error decoding user IDs:", err)
		return
	}

	userIds := request.UserIds

	users, err := uh.service.GetUsersByIds(ctx, userIds)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println("Error retrieving users:", err)
		http.Error(rw, `{"message": "`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	if users == nil || len(users) == 0 {
		span.RecordError(errors.New("User not found"))
		span.SetStatus(codes.Error, "User not found")
		http.Error(rw, "No users found", http.StatusNotFound)
		return
	}

	rw.Header().Set(contentType, appJson)
	rw.WriteHeader(http.StatusOK)
	err = json.NewEncoder(rw).Encode(users)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(responseErr, err)
		http.Error(rw, jsonConvert, http.StatusInternalServerError)
		return
	}
	span.SetStatus(codes.Ok, " users found")
}

func createTLSClient() (*http.Client, error) {
	caCert, err := ioutil.ReadFile("/app/cert.crt")
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	client := &http.Client{
		Transport: transport,
	}

	return client, nil
}
func (uh *UserHandler) HandleGettingRole(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.HandleGettingRole")
	defer span.End()

	type RequestPayload struct {
		Email string `json:"email"`
	}

	var payload RequestPayload
	err := json.NewDecoder(h.Body).Decode(&payload)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, reqBodyErr)
		http.Error(rw, reqBodyErr, http.StatusBadRequest)
		return
	}

	if payload.Email == "" {
		span.RecordError(errors.New(emailRequired))
		span.SetStatus(codes.Error, emailRequired)
		http.Error(rw, emailRequired, http.StatusBadRequest)
		return
	}

	role, err := uh.service.GetRoleForMagic(ctx, payload.Email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, "There has been an error", http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusOK)
	rw.Header().Set(contentType, appJson)
	rw.WriteHeader(http.StatusOK)
	err = json.NewEncoder(rw).Encode(role)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(responseErr, err)
		http.Error(rw, jsonConvert, http.StatusInternalServerError)
	}
	span.SetStatus(codes.Ok, " role found")

}

func (uh *UserHandler) HandleAccountVerification(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.HandleAccountVerification")
	defer span.End()

	email := mux.Vars(h)["email"]
	if len(email) == 0 {
		span.RecordError(errors.New(emailRequired))
		span.SetStatus(codes.Error, emailRequired)
		http.Error(rw, emailRequired, http.StatusBadRequest)
		return
	}

	err := uh.service.VerifyAccount(ctx, email)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(rw, "There has been an error", http.StatusInternalServerError)
		return
	}

	span.SetStatus(codes.Ok, " account verification successful ")
	rw.WriteHeader(http.StatusOK)

}

func (uh *UserHandler) GetUserIdFromToken(rw http.ResponseWriter, h *http.Request) {
	ctx, span := uh.tracer.Start(h.Context(), "UserHandler.ValidateToken")
	defer span.End()
	uh.logger.Println("The path is hit")

	cookie, err := h.Cookie("auth_token")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println("Error getting cookie:", err)
		http.Error(rw, "No token provided", http.StatusBadRequest)
		return
	}

	token := cookie.Value

	userID, _, err := uh.service.ValidateToken(ctx, token)
	uh.logger.Println("User ID is:", userID)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(tokenValidation, err)
		http.Error(rw, `{"message": "Invalid token"}`, http.StatusUnauthorized)
		return
	}

	rw.Header().Set(contentType, "text/plain")
	rw.WriteHeader(http.StatusOK)

	_, err = rw.Write([]byte(userID))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		uh.logger.Println(responseErr, err)
		http.Error(rw, "Internal Server Error", http.StatusInternalServerError)
	}

	span.SetStatus(codes.Ok, "Successfully validated token")
}
