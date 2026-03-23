package monitor

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Status holds pipeline status information.
type Status struct {
	Queries  TableStatus   `json:"queries"`
	Seeds    TableStatus   `json:"seeds"`
	Websites TableStatus   `json:"websites"`
	Contacts TableStatus   `json:"contacts"`
	Queues   []QueueStatus `json:"queues"`
	Dedup    []DedupStatus `json:"dedup"`
}

// TableStatus shows counts per status for a table.
type TableStatus struct {
	Total      int            `json:"total"`
	ByStatus   map[string]int `json:"by_status"`
}

// QueueStatus shows the depth of a Redis queue.
type QueueStatus struct {
	Name  string `json:"name"`
	Depth int64  `json:"depth"`
}

// DedupStatus shows the size of a Redis dedup set.
type DedupStatus struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// GetStatus collects current pipeline status from PG and Redis.
func GetStatus(db *sql.DB, redisClient *redis.Client) (*Status, error) {
	ctx := context.Background()
	s := &Status{}

	// Query counts by status.
	s.Queries = getTableStatus(db, "queries")
	s.Seeds = getTableStatus(db, "serp_seeds")
	s.Websites = getTableStatus(db, "websites")
	s.Contacts = getTableStatus(db, "contacts")

	// Queue depths.
	queues := []string{"serp:queue:queries", "serp:queue:websites", "serp:queue:contacts"}
	for _, q := range queues {
		depth, _ := redisClient.ZCard(ctx, q).Result()
		s.Queues = append(s.Queues, QueueStatus{Name: q, Depth: depth})
	}

	// Dedup set sizes.
	dedups := []string{"serp:dedup:urls", "serp:dedup:domains", "serp:dedup:emails"}
	for _, d := range dedups {
		size, _ := redisClient.SCard(ctx, d).Result()
		s.Dedup = append(s.Dedup, DedupStatus{Name: d, Size: size})
	}

	return s, nil
}

func getTableStatus(db *sql.DB, table string) TableStatus {
	ts := TableStatus{ByStatus: make(map[string]int)}

	// Total count.
	row := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table))
	row.Scan(&ts.Total)

	// Count by status.
	rows, err := db.Query(fmt.Sprintf("SELECT status, COUNT(*) FROM %s GROUP BY status", table))
	if err != nil {
		return ts
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int
		if rows.Scan(&status, &count) == nil {
			ts.ByStatus[status] = count
		}
	}
	return ts
}

