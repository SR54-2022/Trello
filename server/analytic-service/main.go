package main

import (
	config2 "analytics-service/config"
	"analytics-service/customLogger"
	"analytics-service/handlers"
	"analytics-service/repository"
	"context"
	"fmt"
	"github.com/EventStore/EventStore-Client-Go/esdb"
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
	"os"
	"os/signal"
	"time"
)

func main() {
	fmt.Println("Hello from analytic-service")

	config := loadConfig()
	logger := log.New(os.Stdout, "ANALYTICS-SERVICE: ", log.LstdFlags)

	timeoutContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	configuration := os.Getenv("JAEGER_ADDRESS")
	exp, err := newExporter(configuration)
	if err != nil {
		log.Fatalf("Jaeger exporter initialization failed: %v", err)
	} else {
		log.Println("Jaeger initialization succeeded")
	}
	log.Printf("Using JAEGER_ADDRESS: %s", configuration)
	tp := newTraceProvider(exp)
	defer func() { _ = tp.Shutdown(timeoutContext) }()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tracer := tp.Tracer("notification-service")

	cfg := config2.NewConfig()

	connString := fmt.Sprintf("esdb://%s:%s@%s:%s?tls=false", cfg.ESDBUser, cfg.ESDBPass, cfg.ESDBHost, cfg.ESDBPort)

	fmt.Println("Connecting to EventStore with: ", connString)

	settings, err := esdb.ParseConnectionString(connString)
	if err != nil {
		log.Fatal(err)
	}

	client, err := esdb.NewClient(settings)
	if err != nil {
		logger.Fatal("Error initializing EventStore client: ", err)
	} else {
		logger.Println("Successfully connected to EventStore!")
	}

	esdbClient, err := repository.NewESDBClient(client, "analytics-group", tracer)
	if err != nil {
		log.Fatal("Error initializing ESDBClient:", err)
	}

	eventHandler := handlers.NewEventHandler(esdbClient, tracer, logger)
	repo, err := repository.NewAnalyticsRepo(timeoutContext, logger, tracer)
	custLogger := customLogger.GetLogger()

	analyticsHandler := handlers.NewAnalyticsHandler(logger, repo, custLogger, tracer)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Println("Recovered in NotificationListener:", r)
			}
		}()
		analyticsHandler.NotificationListener()
		logger.Println("invoked successfully")

	}()

	r := mux.NewRouter()
	r.Use(handlers.ExtractTraceInfoMiddleware)
	r.HandleFunc("/event/append", eventHandler.ProcessEventHandler).Methods("POST")
	r.Handle("/events/{projectID}", analyticsHandler.MiddlewareExtractUserFromCookie(analyticsHandler.MiddlewareCheckRoles([]string{"manager"}, http.HandlerFunc(eventHandler.GetEventsHandler)))).Methods("GET")
	r.Handle("/analytics/{projectID}", analyticsHandler.MiddlewareExtractUserFromCookie(analyticsHandler.MiddlewareCheckRoles([]string{"member"}, http.HandlerFunc(analyticsHandler.GetProjectAnalyticsHandler)))).Methods("GET")

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
			semconv.ServiceNameKey.String("analytic-service"),
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
