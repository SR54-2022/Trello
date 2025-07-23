package config

import (
	"fmt"
	"os"
)

type Config struct {
	Port             string
	ESDBUser         string
	ESDBPass         string
	ESDBHost         string
	ESDBPort         string
	QueryServiceHost string
	QueryServicePort string
}

func NewConfig() *Config {
	cfg := &Config{
		ESDBHost: os.Getenv("ESDB_HOST"),
		ESDBPort: os.Getenv("ESDB_PORT"),
		ESDBUser: os.Getenv("ESDB_USER"),
		ESDBPass: os.Getenv("ESDB_PASS"),
	}

	fmt.Println("Loaded ESDB Config:")
	fmt.Printf("  Host: %s\n", cfg.ESDBHost)
	fmt.Printf("  Port: %s\n", cfg.ESDBPort)
	fmt.Printf("  User: %s\n", cfg.ESDBUser)

	return cfg
}
