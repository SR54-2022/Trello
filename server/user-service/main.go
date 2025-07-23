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
	"os"
	"os/signal"
	"time"
	"user-service/customLogger"
	"user-service/handlers"
	"user-service/repository"
	"user-service/service"
)

func main() {
	config := loadConfig()
	timeoutContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	logger := log.New(os.Stdout, "[user-api] ", log.LstdFlags)
	cfg := os.Getenv("JAEGER_ADDRESS")
	exp, err := newExporter(cfg)
	if err != nil {
		log.Fatal(err)
	}
	tp := newTraceProvider(exp)
	defer func() { _ = tp.Shutdown(timeoutContext) }()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tracer := tp.Tracer("user-service")
	custLogger := customLogger.GetLogger()

	ur, err := repository.New(timeoutContext, logger, custLogger, tracer)

	if err != nil {
		logger.Fatal(err)
	}
	uc, err := repository.NewCache(logger, ur, tracer, custLogger)
	us := service.NewUserService(ur, uc, logger, tracer)
	uh := handlers.NewUserHandler(logger, us, tracer, custLogger)

	r := mux.NewRouter()
	r.Use(handlers.ExtractTraceInfoMiddleware)

	r.Handle("/register", uh.MiddlewareCheckAuthenticated(http.HandlerFunc(uh.Registration))).Methods(http.MethodPost)
	r.Handle("/register", uh.MiddlewareCheckAuthenticated(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // FOR OPTIONS METHOD
	}))).Methods(http.MethodOptions)
	r.Handle("/login", uh.MiddlewareCheckAuthenticated(http.HandlerFunc(uh.Login))).Methods(http.MethodPost)

	r.Handle("/members", uh.MiddlewareExtractUserFromCookie(uh.MiddlewareCheckRoles([]string{"manager", "member"}, http.HandlerFunc(uh.GetAllMembers)))).Methods(http.MethodGet)
	r.Handle("/manager", uh.MiddlewareExtractUserFromCookie(http.HandlerFunc(uh.GetManager)))
	r.Handle("/user", uh.MiddlewareExtractUserFromCookie(http.HandlerFunc(uh.DeleteUser))).Methods(http.MethodDelete) // for deleting account

	r.Handle("/verify", uh.MiddlewareExtractUserFromCookie(uh.MiddlewareCheckRoles([]string{"manager", "member"}, http.HandlerFunc(uh.VerifyTokenExistence)))).Methods(http.MethodGet)
	r.Handle("/logout", uh.MiddlewareExtractUserFromCookie(uh.MiddlewareCheckRoles([]string{"manager", "member"}, http.HandlerFunc(uh.Logout))))
	r.Handle("/password/check", uh.MiddlewareExtractUserFromCookie(uh.MiddlewareCheckRoles([]string{"manager", "member"}, http.HandlerFunc(uh.CheckPasswords))))
	r.Handle("/password/change", uh.MiddlewareExtractUserFromCookie(uh.MiddlewareCheckRoles([]string{"manager", "member"}, http.HandlerFunc(uh.ChangePassword))))
	r.Handle("/users/details", uh.MiddlewareExtractUserFromCookie(uh.MiddlewareCheckRoles([]string{"manager", "member"}, http.HandlerFunc(uh.GetUsersByIds)))).Methods("POST")

	r.Handle("/password/recovery", uh.MiddlewareCheckAuthenticated(http.HandlerFunc(uh.HandleRecovery))).Methods(http.MethodPost)
	r.Handle("/password/reset", uh.MiddlewareCheckAuthenticated(http.HandlerFunc(uh.HandlePasswordReset))).Methods(http.MethodPost)
	r.Handle("/magic", uh.MiddlewareCheckAuthenticated(http.HandlerFunc(uh.HandleMagic))).Methods(http.MethodPost)
	r.Handle("/magic/verify", uh.MiddlewareCheckAuthenticated(http.HandlerFunc(uh.HandleMagicVerification))).Methods(http.MethodPost)
	r.HandleFunc("/role", uh.HandleGettingRole).Methods(http.MethodPost)
	r.HandleFunc("/verify/account/{email}", uh.HandleAccountVerification).Methods(http.MethodGet)

	// SAMO IM SERVIS PRISTUPA
	r.HandleFunc("/validate-token", uh.ValidateToken).Methods(http.MethodPost)
	r.HandleFunc("/managers", uh.GetManagers).Methods(http.MethodGet) //GDE SE UOPSTE POZIVA -- VISE NIGDE, pozivalo se kod pravljenja projekta(pre logina) da se popune menageri u dropdown listi
	r.HandleFunc("/userId", uh.GetUserIdFromToken).Methods(http.MethodGet)

	corsHandler := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"*"},
		AllowCredentials: true,
	})

	handler := corsHandler.Handler(r)

	server := http.Server{
		Addr:         config["address"],
		Handler:      handler,
		IdleTimeout:  120 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	logger.Println("Server listening on port", config["address"])

	go func() {
		err := server.ListenAndServeTLS("/app/cert.crt", "/app/privat.key")
		//err := server.ListenAndServe()
		if err != nil {
			logger.Fatal(err)
		}
	}()

	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt)
	signal.Notify(sigCh, os.Kill)

	sig := <-sigCh
	logger.Println("Received terminate, graceful shutdown", sig)
	if server.Shutdown(timeoutContext) != nil {
		logger.Fatal("Cannot gracefully shutdown...")
	}
	logger.Println("Server stopped")
}

func loadConfig() map[string]string {
	config := make(map[string]string)
	config["address"] = fmt.Sprintf(":%s", os.Getenv("PORT"))
	return config
}

func newExporter(address string) (*jaeger.Exporter, error) {
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(address)))
	if err != nil {
		return nil, err
	}
	return exp, nil
}

func newTraceProvider(exp sdktrace.SpanExporter) *sdktrace.TracerProvider {
	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("user-service"),
		),
	)
	if err != nil {
		log.Fatalf("failed to create resource: %v", err)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(r),
	)
}
