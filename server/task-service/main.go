package main

import (
	"context"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/nats-io/nats.go"
	"github.com/rs/cors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"log"
	"net/http"
	"os"
	"os/signal"
	"task--service/client"
	"task--service/customLogger"
	"task--service/handlers"
	"task--service/repositories"
	"time"
)

func initUserClient() client.UserClient {
	return client.NewUserClient(os.Getenv("USER_SERVICE_HOST"), os.Getenv("PORT"))
}

func main() {

	const (
		taskFail = "Failed to subscribe to ProjectDeleted: %v"
	)
	fmt.Println("Task Service is starting...")

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://nats:4222"
	}
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()

	port := os.Getenv("PORT")
	if len(port) == 0 {
		port = "8080"
	}

	custLogger := customLogger.GetLogger()

	timeoutContext, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	logger := log.New(os.Stdout, "[task-api] ", log.LstdFlags)
	storeLogger := log.New(os.Stdout, "[task-store] ", log.LstdFlags)
	cfg := os.Getenv("JAEGER_ADDRESS")
	exp, err := newExporter(cfg)
	if err != nil {
		log.Fatalf("Jaeger exporter initialization failed: %v", err)
	} else {
		log.Println("Jaeger initialization succeeded")
	}
	log.Printf("Using JAEGER_ADDRESS: %s", cfg)
	tp := newTraceProvider(exp)
	defer func() { _ = tp.Shutdown(timeoutContext) }()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tracer := tp.Tracer("task-service")

	store, err := repositories.New(timeoutContext, storeLogger, tracer)
	if err != nil {
		logger.Fatal(err)
	}
	defer store.Disconnect(timeoutContext)

	store.Ping()
	userClient := initUserClient()

	taskDocStore, err := repositories.NewTaskDocumentRepository(timeoutContext, storeLogger, tracer)
	if err != nil {
		logger.Fatal(err)
	}
	defer store.Disconnect(timeoutContext)

	taskHandler := handlers.NewTasksHandler(logger, store, taskDocStore, nc, tracer, userClient, custLogger)

	sub, err := nc.QueueSubscribe("ProjectDeleted", "task-queue", func(msg *nats.Msg) {
		projectID := string(msg.Data)
		ctx := context.Background()
		taskHandler.HandleProjectDeleted(ctx, projectID)
	})
	if err != nil {
		logger.Fatalf(taskFail, err)
	}
	defer sub.Unsubscribe()

	sub2, err := nc.Subscribe("TasksDeletionComplete", func(msg *nats.Msg) {
		projectID := string(msg.Data)
		ctx := context.Background()
		taskHandler.DeletedTasks(ctx, projectID)
	})
	if err != nil {
		logger.Fatalf(taskFail, err)
	}
	defer sub2.Unsubscribe()

	sub3, err := nc.Subscribe("WorkflowsDeletionFailed", func(msg *nats.Msg) {
		projectID := string(msg.Data)
		ctx := context.Background()
		taskHandler.RollbackTasks(ctx, projectID)
	})
	if err != nil {
		logger.Fatalf(taskFail, err)
	}
	defer sub3.Unsubscribe()

	defer func() {
		if err := nc.Drain(); err != nil {
			logger.Printf("Error draining NATS connection: %v", err)
		}
		nc.Close()
	}()

	router := mux.NewRouter()

	router.Use(taskHandler.MiddlewareContentTypeSet)
	router.Use(handlers.ExtractTraceInfoMiddleware)

	getRouter := router.Methods(http.MethodGet).Subrouter()
	getRouter.Handle("/tasks", taskHandler.MiddlewareExtractUserFromCookie(taskHandler.MiddlewareCheckRoles([]string{"manager", "member"}, http.HandlerFunc(taskHandler.GetAllTask))))
	getRouter.Handle("/tasks/{projectId}", taskHandler.MiddlewareExtractUserFromCookie(taskHandler.MiddlewareCheckRoles([]string{"member", "manager"}, http.HandlerFunc(taskHandler.GetAllTasksByProjectId))))
	getRouter.Handle("/tasksDetails/{projectId}", taskHandler.MiddlewareExtractUserFromCookie(taskHandler.MiddlewareCheckRoles([]string{"member", "manager"}, http.HandlerFunc(taskHandler.GetAllTasksDetailsByProjectId))))
	getRouter.Handle("/tasks/{taskId}/status", http.HandlerFunc(taskHandler.GetStatusForTask))

	postPutRouter := router.Methods(http.MethodPost, http.MethodPut).Subrouter()
	postPutRouter.Handle("/tasks", taskHandler.MiddlewareExtractUserFromCookie(taskHandler.MiddlewareCheckRoles([]string{"manager"}, http.HandlerFunc(taskHandler.PostTask))))

	postPutRouter.Use(taskHandler.MiddlewareTaskDeserialization)
	router.Handle("/tasks/status", taskHandler.MiddlewareExtractUserFromCookie(taskHandler.MiddlewareCheckRoles([]string{"manager", "member"}, http.HandlerFunc(taskHandler.HandleStatusUpdate)))).Methods("PUT")
	router.HandleFunc("/tasks/{taskId}/members/{action}/{userId}", taskHandler.LogTaskMemberChange).Methods("POST")
	postPutRouter.Handle("/tasks/check", taskHandler.MiddlewareExtractUserFromCookie(taskHandler.MiddlewareCheckRoles([]string{"manager", "member"}, http.HandlerFunc(taskHandler.HandleCheckingIfUserIsInTask))))
	postPutRouter.Handle("/tasks/{taskId}/block", http.HandlerFunc(taskHandler.BlockTask)).Methods(http.MethodPost)
	postPutRouter.Handle("/tasks/{taskId}/dependency/{dependencyId}", http.HandlerFunc(taskHandler.AddDependencyToTask)).Methods(http.MethodPost)

	documentRouter := router.Methods(http.MethodPost).Subrouter()
	documentRouter.Handle("/tasks/upload", taskHandler.MiddlewareExtractUserFromCookie(taskHandler.MiddlewareCheckRoles([]string{"manager", "member"}, http.HandlerFunc(taskHandler.UploadTaskDocument))))

	documentGetRouter := router.Methods(http.MethodGet).Subrouter()
	documentGetRouter.Handle("/tasks/getUploads/{taskId}", taskHandler.MiddlewareExtractUserFromCookie(taskHandler.MiddlewareCheckRoles([]string{"manager", "member"}, http.HandlerFunc(taskHandler.GetTaskDocumentsByTaskID))))
	documentGetRouter.Handle("/tasks/download/{taskDocumentId}", taskHandler.MiddlewareExtractUserFromCookie(taskHandler.MiddlewareCheckRoles([]string{"manager", "member"}, http.HandlerFunc(taskHandler.DownloadTaskDocument))))

	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"*"},
		AllowCredentials: true,
	})

	handler := corsHandler.Handler(router)

	server := http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		IdleTimeout:  120 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Println("Server listening on port", port)
		err := server.ListenAndServeTLS("/app/cert.crt", "/app/privat.key")
		//err := server.ListenAndServe()

		if err != nil {
			logger.Fatal(err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, os.Kill)

	sig := <-sigCh
	logger.Println("Received terminate signal, shutting down...", sig)

	if err := server.Shutdown(timeoutContext); err != nil {
		logger.Fatal("Could not gracefully shutdown the server", err)
	}
	logger.Println("Server stopped successfully")
}

func newExporter(address string) (*jaeger.Exporter, error) {
	if address == "" {
		return nil, fmt.Errorf("jaeger collector endpoint address is empty")
	}
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(address)))
	if err != nil {
		return nil, fmt.Errorf("failed to create Jaeger exporter: %w", err)
	}
	return exp, nil
}

func newTraceProvider(exp sdktrace.SpanExporter) *sdktrace.TracerProvider {
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("task-service"),
		),
	)
	if err != nil {
		log.Fatalf("failed to create resource: %v", err)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)
}
