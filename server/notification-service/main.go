package main

import (
	"context"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"log"
	"net/http"
	"notification-service/customLogger"
	handler "notification-service/handlers"
	"notification-service/repository"
	"os"
	"os/signal"
	"time"
)

func main() {
	fmt.Println("Hello from notification-service")

	const (
		notifId = "/notifications/{id}"
	)

	config := loadConfig()
	logger := log.New(os.Stdout, "NOTIFICATION-SERVICE: ", log.LstdFlags)

	timeoutContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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
	custLogger := customLogger.GetLogger()
	tracer := tp.Tracer("notification-service")

	repo, err := repository.New(logger, tracer)
	if err != nil {
		logger.Fatal("Error initializing repository:", err)
	}

	repo.CreateTables()

	notificationHandler := handler.NewNotificationHandler(logger, repo, custLogger, tracer)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Println("Recovered in NotificationListener:", r)
			}
		}()
		notificationHandler.NotificationListener()
		logger.Println("invoked successfully")

	}()
	r := mux.NewRouter()
	r.Use(handler.ExtractTraceInfoMiddleware)

	r.Handle("/notifications/unread-count", notificationHandler.MiddlewareExtractUserFromCookie(notificationHandler.MiddlewareCheckRoles([]string{"member"}, http.HandlerFunc(notificationHandler.GetUnreadNotificationCount)))).Methods("GET")
	r.Handle("/notifications", notificationHandler.MiddlewareExtractUserFromCookie(notificationHandler.MiddlewareCheckRoles([]string{"member"}, http.HandlerFunc(notificationHandler.CreateNotification)))).Methods("POST")
	r.Handle(notifId, notificationHandler.MiddlewareExtractUserFromCookie(notificationHandler.MiddlewareCheckRoles([]string{"member"}, http.HandlerFunc(notificationHandler.GetNotificationByID)))).Methods("GET")
	r.Handle("/notifications", notificationHandler.MiddlewareExtractUserFromCookie(notificationHandler.MiddlewareCheckRoles([]string{"member"}, http.HandlerFunc(notificationHandler.GetNotificationsByUserID))))
	r.Handle(notifId, notificationHandler.MiddlewareExtractUserFromCookie(notificationHandler.MiddlewareCheckRoles([]string{"member"}, http.HandlerFunc(notificationHandler.UpdateNotificationStatus)))).Methods("PUT")
	r.Handle(notifId, notificationHandler.MiddlewareExtractUserFromCookie(notificationHandler.MiddlewareCheckRoles([]string{"member"}, http.HandlerFunc(notificationHandler.DeleteNotification)))).Methods("DELETE")

	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"*"},
		AllowCredentials: true,
	})

	handler := corsHandler.Handler(r)

	server := &http.Server{
		Addr:         config["address"],
		Handler:      handler,
		IdleTimeout:  120 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Println("Server listening on", config["address"])
		if err := server.ListenAndServeTLS("/app/cert.crt", "/app/privat.key"); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Server failed: ", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, os.Kill)

	sig := <-sigCh
	logger.Println("Received terminate signal, shutting down gracefully...", sig)

	if err := server.Shutdown(timeoutContext); err != nil {
		logger.Fatal("Error during graceful shutdown:", err)
	}
	logger.Println("Server gracefully stopped")
}

func loadConfig() map[string]string {
	config := make(map[string]string)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	config["address"] = fmt.Sprintf(":%s", port)

	return config
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
			semconv.ServiceNameKey.String("notification-service"),
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
