package agent

import (
	"fmt"
	"github.com/joho/godotenv"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Config holds all configuration parameters
type Config struct {
	Server   ServerConfig
	Database DBConfig
	SignOz   SignOzConfig
	Query    QueryConfig
}

// ServerConfig holds server-related configuration
type ServerConfig struct {
	Port        int
	WebhookPath string
}

// DBConfig holds database configuration
type DBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

// SignOzConfig holds SignOz API-related configuration
type SignOzConfig struct {
	APIURL string
	APIKey string
}

// QueryConfig holds query-related configuration
type QueryConfig struct {
	Step        int64
	WindowHours int
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {

	pwd, err := os.Getwd()
	if err != nil {
		panic(err)
	} // Load .env file

	if err := godotenv.Load(filepath.Join(pwd, ".env")); err != nil {
		return nil, fmt.Errorf("error loading .env file: %v", err)
	}

	// Server configuration
	port, err := strconv.Atoi(getEnvOrDefault("PORT", "8080"))
	if err != nil {
		return nil, fmt.Errorf("error parsing PORT: %v", err)
	}

	// Database configuration
	dbPort, err := strconv.Atoi(getEnvOrDefault("DB_PORT", "5432"))
	if err != nil {
		return nil, fmt.Errorf("error parsing DB_PORT: %v", err)
	}

	// Query configuration
	queryStep, err := strconv.ParseInt(getEnvOrDefault("QUERY_STEP", "60"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing QUERY_STEP: %v", err)
	}

	queryWindowHours, err := strconv.Atoi(getEnvOrDefault("QUERY_WINDOW_HOURS", "1"))
	if err != nil {
		return nil, fmt.Errorf("error parsing QUERY_WINDOW_HOURS: %v", err)
	}

	return &Config{
		Server: ServerConfig{
			Port:        port,
			WebhookPath: getEnvOrDefault("WEBHOOK_PATH", "/webhook"),
		},
		Database: DBConfig{
			Host:     getEnvOrDefault("DB_HOST", "localhost"),
			Port:     dbPort,
			User:     getEnvOrDefault("DB_USER", "postgres"),
			Password: getEnvOrDefault("DB_PASSWORD", "postgres"),
			DBName:   getEnvOrDefault("DB_NAME", "signoz_alerts"),
			SSLMode:  getEnvOrDefault("DB_SSL_MODE", "disable"),
		},
		SignOz: SignOzConfig{
			APIURL: getEnvOrDefault("SIGNOZ_API_URL", ""),
			APIKey: getEnvOrDefault("SIGNOZ_API_KEY", ""),
		},
		Query: QueryConfig{
			Step:        queryStep,
			WindowHours: queryWindowHours,
		},
	}, nil
}

// getEnvOrDefault returns the value of an environment variable or a default value
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// GetQueryTimeRange returns the start and end time for a query based on configuration
func (c *Config) GetQueryTimeRange() (int64, int64) {
	now := time.Now()
	end := now.Unix()
	start := now.Add(-time.Duration(c.Query.WindowHours) * time.Hour).Unix()
	return start, end
}
