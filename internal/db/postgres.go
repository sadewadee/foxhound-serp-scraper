package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"

	"github.com/sadewadee/serp-scraper/internal/config"
)

// Open creates a PostgreSQL connection pool using the config DSN.
func Open(cfg *config.Config) (*sql.DB, error) {
	dsn := cfg.DSN()
	if dsn == "" {
		return nil, fmt.Errorf("db: postgres DSN is empty (set postgres.dsn in config or POSTGRES_DSN env)")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("db: opening connection: %w", err)
	}

	db.SetMaxOpenConns(cfg.Postgres.MaxOpenConns)
	db.SetMaxIdleConns(cfg.Postgres.MaxIdleConns)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("db: ping failed: %w", err)
	}

	return db, nil
}
