package api

import (
	"context"
	"net/http"
	"time"
)

// dashboardStatsCacheTTL is the Redis cache window for /api/v2/stats.
// Dashboard polling clients re-hit within this window for ~free.
const dashboardStatsCacheTTL = 30 * time.Second

// handleV2DashboardStats returns the full dashboard stats wrapped in V2 format.
// Backed by a 30s Redis cache with single-flight to prevent thundering herd
// when the cache TTL expires under polling load. A compute error (no figure
// could be produced) is never cached.
func (s *Server) handleV2DashboardStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := v2RequestContext(r)
	defer cancel()

	s.cachedJSON(ctx, w,
		"v2:stats:dashboard", dashboardStatsCacheTTL, 5*time.Second,
		func() (any, error) {
			return s.v2DashboardStats(ctx)
		},
	)
}

// v2DashboardStats shapes computeDashboardStats for V2. The envelope
// {"data": ...} is constructed inline because cachedJSON caches raw JSON bytes;
// going through writeV2Single inside the cached path would re-wrap on every
// cache miss.
func (s *Server) v2DashboardStats(ctx context.Context) (map[string]any, error) {
	st, err := s.computeDashboardStats(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"data": map[string]any{
			"queries":           st.Queries,
			"serp":              st.Serp,
			"enrich":            st.Enrich,
			"results":           st.Results,
			"queues":            st.Queues,
			"unknown_figures":   nonNil(st.Unknown),
			"estimated_figures": nonNil(st.Estimated),
			"snapshot_at":       st.SnapshotAt,
		},
	}, nil
}

// nonNil keeps empty lists serializing as [] rather than null.
func nonNil(l []string) []string {
	if l == nil {
		return []string{}
	}
	return l
}
