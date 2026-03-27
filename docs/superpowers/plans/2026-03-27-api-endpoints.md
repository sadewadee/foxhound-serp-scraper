# API Endpoints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add missing API endpoints so scraping jobs can be managed entirely via HTTP — no SSH/exec needed.

**Architecture:** All new handlers go in `internal/api/server.go`, following the existing pattern: inline SQL, `writeJSON` responses, `RequireRole` middleware. The `query.Repository` is added as a dependency to `Server` for query write operations. No new packages needed.

**Tech Stack:** Go stdlib `net/http`, `database/sql`, `github.com/redis/go-redis/v9`

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `internal/api/server.go` | Modify | Add new handlers + routes, add `queryRepo` field to Server |
| `internal/query/repository.go` | Modify | Add `DeleteByIDs`, `RetryErrors` methods |
| `cmd/run.go` | Modify | Pass `query.Repository` into `NewServer` |

---

### Task 1: Wire query.Repository into API Server

The Server needs access to `query.Repository` for insert/delete operations on queries.

**Files:**
- Modify: `internal/api/server.go:16-38`
- Modify: `cmd/run.go:106-110`

- [ ] **Step 1: Add queryRepo field to Server struct and NewServer**

In `internal/api/server.go`, add `queryRepo` field and update constructor:

```go
import (
	"github.com/sadewadee/serp-scraper/internal/query"
)

type Server struct {
	db        *sql.DB
	redis     *redis.Client
	auth      *Auth
	dokploy   *DokployClient
	queryRepo *query.Repository
	workers   map[string]*WorkerInfo
	mux       *http.ServeMux
	server    *http.Server
}

func NewServer(db *sql.DB, redisClient *redis.Client, authCfg AuthConfig, dokployCfg DokployConfig) *Server {
	s := &Server{
		db:        db,
		redis:     redisClient,
		auth:      NewAuth(authCfg),
		dokploy:   NewDokployClient(dokployCfg),
		queryRepo: query.NewRepository(db),
		workers:   make(map[string]*WorkerInfo),
		mux:       http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}
```

- [ ] **Step 2: Verify build compiles**

Run: `cd /Users/sadewadee/Downloads/Plugin\ Pro/serp-scraper && go build -tags playwright -o /dev/null .`
Expected: clean build, no errors

- [ ] **Step 3: Commit**

```bash
git add internal/api/server.go
git commit -m "Wire query.Repository into API server"
```

---

### Task 2: POST /api/queries — Submit keywords

Accept a JSON array of keyword strings, insert via `queryRepo.InsertBatch`, return count of new queries inserted.

**Files:**
- Modify: `internal/api/server.go` (add route + handler)

- [ ] **Step 1: Add route registration**

In `registerRoutes()`, under the existing `GET /api/queries` line:

```go
	s.mux.HandleFunc("GET /api/queries", RequireRole(RoleViewer, s.handleListQueries))
	s.mux.HandleFunc("POST /api/queries", RequireRole(RoleAdmin, s.handleCreateQueries))
```

- [ ] **Step 2: Add handler**

```go
func (s *Server) handleCreateQueries(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Queries []string `json:"queries"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	// Filter empty strings.
	var cleaned []string
	for _, q := range req.Queries {
		q = strings.TrimSpace(q)
		if q != "" {
			cleaned = append(cleaned, q)
		}
	}
	if len(cleaned) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "queries array is empty"})
		return
	}

	inserted, err := s.queryRepo.InsertBatch(cleaned, "")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"inserted":   inserted,
		"duplicates": len(cleaned) - inserted,
		"total":      len(cleaned),
	})
}
```

- [ ] **Step 3: Add `"strings"` import if not already present**

Check imports at top of `server.go`, add `"strings"` if missing.

- [ ] **Step 4: Verify build compiles**

Run: `cd /Users/sadewadee/Downloads/Plugin\ Pro/serp-scraper && go build -tags playwright -o /dev/null .`

- [ ] **Step 5: Commit**

```bash
git add internal/api/server.go
git commit -m "Add POST /api/queries endpoint for submitting keywords"
```

---

### Task 3: DELETE /api/queries — Delete queries by IDs

Accept a JSON array of query IDs, delete them from the database.

**Files:**
- Modify: `internal/query/repository.go` (add DeleteByIDs method)
- Modify: `internal/api/server.go` (add route + handler)

- [ ] **Step 1: Add DeleteByIDs to repository**

In `internal/query/repository.go`:

```go
// DeleteByIDs deletes queries by their IDs. Returns the number of rows deleted.
func (r *Repository) DeleteByIDs(ids []int64) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	// Build $1, $2, ... placeholders.
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	query := fmt.Sprintf("DELETE FROM queries WHERE id IN (%s)", strings.Join(placeholders, ","))
	res, err := r.db.Exec(query, args...)
	if err != nil {
		return 0, fmt.Errorf("query: delete by ids: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
```

Add `"strings"` to the import block in `repository.go`.

- [ ] **Step 2: Add route and handler in server.go**

Route:
```go
	s.mux.HandleFunc("DELETE /api/queries", RequireRole(RoleAdmin, s.handleDeleteQueries))
```

Handler:
```go
func (s *Server) handleDeleteQueries(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs []int64 `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.IDs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ids array is required"})
		return
	}

	deleted, err := s.queryRepo.DeleteByIDs(req.IDs)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
}
```

- [ ] **Step 3: Verify build compiles**

Run: `cd /Users/sadewadee/Downloads/Plugin\ Pro/serp-scraper && go build -tags playwright -o /dev/null .`

- [ ] **Step 4: Commit**

```bash
git add internal/query/repository.go internal/api/server.go
git commit -m "Add DELETE /api/queries endpoint"
```

---

### Task 4: POST /api/queries/retry — Retry failed queries

Reset all `error` status queries back to `pending`.

**Files:**
- Modify: `internal/query/repository.go` (add RetryErrors method)
- Modify: `internal/api/server.go` (add route + handler)

- [ ] **Step 1: Add RetryErrors to repository**

In `internal/query/repository.go`:

```go
// RetryErrors resets all error queries back to pending.
func (r *Repository) RetryErrors() (int, error) {
	res, err := r.db.Exec(`
		UPDATE queries SET status = 'pending', error_msg = '', updated_at = NOW()
		WHERE status = 'error'
	`)
	if err != nil {
		return 0, fmt.Errorf("query: retry errors: %w", err)
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}
```

- [ ] **Step 2: Add route and handler in server.go**

Route:
```go
	s.mux.HandleFunc("POST /api/queries/retry", RequireRole(RoleAdmin, s.handleRetryQueries))
```

Handler:
```go
func (s *Server) handleRetryQueries(w http.ResponseWriter, r *http.Request) {
	retried, err := s.queryRepo.RetryErrors()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"retried": retried})
}
```

- [ ] **Step 3: Verify build compiles**

Run: `cd /Users/sadewadee/Downloads/Plugin\ Pro/serp-scraper && go build -tags playwright -o /dev/null .`

- [ ] **Step 4: Commit**

```bash
git add internal/query/repository.go internal/api/server.go
git commit -m "Add POST /api/queries/retry endpoint"
```

---

### Task 5: GET /api/queries/stats — Query status breakdown

Return count per status (pending, processing, done, error) using existing `CountByStatus`.

**Files:**
- Modify: `internal/api/server.go` (add route + handler)

- [ ] **Step 1: Add route and handler**

Route:
```go
	s.mux.HandleFunc("GET /api/queries/stats", RequireRole(RoleViewer, s.handleQueryStats))
```

Handler:
```go
func (s *Server) handleQueryStats(w http.ResponseWriter, r *http.Request) {
	counts, err := s.queryRepo.CountByStatus()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	total := 0
	for _, c := range counts {
		total += c
	}
	counts["total"] = total

	writeJSON(w, http.StatusOK, counts)
}
```

- [ ] **Step 2: Verify build compiles**

Run: `cd /Users/sadewadee/Downloads/Plugin\ Pro/serp-scraper && go build -tags playwright -o /dev/null .`

- [ ] **Step 3: Commit**

```bash
git add internal/api/server.go
git commit -m "Add GET /api/queries/stats endpoint"
```

---

### Task 6: POST /api/pipeline/reset — Reset dedup sets

Flush Redis dedup sets so pipeline can re-process URLs/emails. Optionally flush queues too.

**Files:**
- Modify: `internal/api/server.go` (add route + handler)

- [ ] **Step 1: Add route and handler**

Route:
```go
	s.mux.HandleFunc("POST /api/pipeline/reset", RequireRole(RoleAdmin, s.handlePipelineReset))
```

Handler:
```go
func (s *Server) handlePipelineReset(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Dedup  bool `json:"dedup"`
		Queues bool `json:"queues"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	if !req.Dedup && !req.Queues {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "specify dedup and/or queues to reset"})
		return
	}

	ctx := context.Background()
	cleared := map[string]int64{}

	if req.Dedup {
		for _, key := range []string{"serp:dedup:urls", "serp:dedup:domains", "serp:dedup:emails"} {
			n, _ := s.redis.SCard(ctx, key).Result()
			s.redis.Del(ctx, key)
			cleared[key] = n
		}
	}

	if req.Queues {
		for _, key := range []string{"serp:queue:websites", "serp:queue:contacts"} {
			n, _ := s.redis.ZCard(ctx, key).Result()
			s.redis.Del(ctx, key)
			cleared[key] = n
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"cleared": cleared})
}
```

- [ ] **Step 2: Verify build compiles**

Run: `cd /Users/sadewadee/Downloads/Plugin\ Pro/serp-scraper && go build -tags playwright -o /dev/null .`

- [ ] **Step 3: Commit**

```bash
git add internal/api/server.go
git commit -m "Add POST /api/pipeline/reset endpoint"
```

---

### Task 7: Improve GET /api/queries — Add pagination and filters

The current `handleListQueries` is hardcoded to `LIMIT 100` with no pagination or filtering.

**Files:**
- Modify: `internal/api/server.go` (update handleListQueries)

- [ ] **Step 1: Update handleListQueries**

Replace the existing `handleListQueries` with:

```go
func (s *Server) handleListQueries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page := queryInt(q, "page", 1)
	perPage := queryInt(q, "per_page", 50)
	if perPage > 200 {
		perPage = 200
	}
	offset := (page - 1) * perPage

	where := "WHERE 1=1"
	args := []any{}
	argIdx := 1

	if status := q.Get("status"); status != "" {
		where += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}
	if search := q.Get("search"); search != "" {
		where += fmt.Sprintf(" AND text ILIKE $%d", argIdx)
		args = append(args, "%"+search+"%")
		argIdx++
	}

	var total int
	s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM queries %s", where), args...).Scan(&total)

	dataQuery := fmt.Sprintf(`
		SELECT id, text, status, result_count, COALESCE(error_msg,''), created_at
		FROM queries %s
		ORDER BY id DESC
		LIMIT $%d OFFSET $%d
	`, where, argIdx, argIdx+1)
	args = append(args, perPage, offset)

	rows, err := s.db.Query(dataQuery, args...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	var queries []map[string]any
	for rows.Next() {
		var id int64
		var text, status, errMsg string
		var resultCount int
		var createdAt time.Time
		rows.Scan(&id, &text, &status, &resultCount, &errMsg, &createdAt)
		queries = append(queries, map[string]any{
			"id": id, "text": text, "status": status,
			"result_count": resultCount, "error": errMsg, "created_at": createdAt,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data":     queries,
		"total":    total,
		"page":     page,
		"per_page": perPage,
	})
}
```

- [ ] **Step 2: Verify build compiles**

Run: `cd /Users/sadewadee/Downloads/Plugin\ Pro/serp-scraper && go build -tags playwright -o /dev/null .`

- [ ] **Step 3: Commit**

```bash
git add internal/api/server.go
git commit -m "Add pagination and filters to GET /api/queries"
```

---

### Task 8: DELETE /api/contacts — Delete contacts

Delete contacts by domain or by ID list.

**Files:**
- Modify: `internal/api/server.go` (add route + handler)

- [ ] **Step 1: Add route and handler**

Route:
```go
	s.mux.HandleFunc("DELETE /api/contacts", RequireRole(RoleAdmin, s.handleDeleteContacts))
```

Handler:
```go
func (s *Server) handleDeleteContacts(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs    []int64 `json:"ids"`
		Domain string  `json:"domain"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	if len(req.IDs) == 0 && req.Domain == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "provide ids or domain"})
		return
	}

	var res sql.Result
	var err error

	if req.Domain != "" {
		res, err = s.db.Exec("DELETE FROM contacts WHERE domain = $1", req.Domain)
	} else {
		placeholders := make([]string, len(req.IDs))
		args := make([]any, len(req.IDs))
		for i, id := range req.IDs {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = id
		}
		res, err = s.db.Exec(
			fmt.Sprintf("DELETE FROM contacts WHERE id IN (%s)", strings.Join(placeholders, ",")),
			args...,
		)
	}

	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	n, _ := res.RowsAffected()
	writeJSON(w, http.StatusOK, map[string]any{"deleted": n})
}
```

- [ ] **Step 2: Verify build compiles**

Run: `cd /Users/sadewadee/Downloads/Plugin\ Pro/serp-scraper && go build -tags playwright -o /dev/null .`

- [ ] **Step 3: Commit**

```bash
git add internal/api/server.go
git commit -m "Add DELETE /api/contacts endpoint"
```

---

### Task 9: Final build verification and push

- [ ] **Step 1: Full build**

Run: `cd /Users/sadewadee/Downloads/Plugin\ Pro/serp-scraper && go build -tags playwright -o /dev/null .`

- [ ] **Step 2: Verify all routes registered**

Check `registerRoutes()` has all new routes:
```
POST   /api/queries          → handleCreateQueries   (Admin)
DELETE /api/queries          → handleDeleteQueries   (Admin)
POST   /api/queries/retry    → handleRetryQueries    (Admin)
GET    /api/queries/stats    → handleQueryStats      (Viewer)
POST   /api/pipeline/reset   → handlePipelineReset   (Admin)
DELETE /api/contacts         → handleDeleteContacts   (Admin)
```

Plus improved: `GET /api/queries` now has pagination + filters.

- [ ] **Step 3: Commit all changes and push**

```bash
git push origin main
```

---

## Final API Surface

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| POST | `/api/auth/login` | None | Login |
| POST | `/api/auth/refresh` | Bearer | Refresh token |
| GET | `/api/health` | None | Health check |
| **POST** | **`/api/queries`** | **Admin** | **Submit keywords** |
| GET | `/api/queries` | Viewer | List queries (paginated + filterable) |
| GET | `/api/queries/stats` | Viewer | Query status breakdown |
| **DELETE** | **`/api/queries`** | **Admin** | **Delete queries by IDs** |
| **POST** | **`/api/queries/retry`** | **Admin** | **Retry failed queries** |
| GET | `/api/contacts` | Viewer | List contacts |
| GET | `/api/contacts/export` | Viewer | Export CSV/JSON |
| GET | `/api/contacts/stats` | Viewer | Contact stats |
| **DELETE** | **`/api/contacts`** | **Admin** | **Delete contacts** |
| GET | `/api/domains` | Viewer | Domain list |
| GET | `/api/categories` | Viewer | Categories |
| GET | `/api/pipeline/stats` | Viewer | Pipeline stats |
| **POST** | **`/api/pipeline/reset`** | **Admin** | **Reset dedup/queues** |
| POST | `/api/workers/deploy` | Admin | Deploy worker |
| GET | `/api/workers` | Viewer | List workers |
| POST | `/api/workers/stop` | Admin | Stop worker |
| POST | `/api/workers/start` | Admin | Start worker |
| DELETE | `/api/workers` | Admin | Delete worker |
