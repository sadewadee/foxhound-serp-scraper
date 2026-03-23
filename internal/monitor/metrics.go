//go:build playwright

package monitor

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds Prometheus counters for each stage.
type Metrics struct {
	QueriesProcessed prometheus.Counter
	SERPURLsFound    prometheus.Counter
	DomainsProcessed prometheus.Counter
	PagesQueued      prometheus.Counter
	PagesProcessed   prometheus.Counter
	EmailsFound      prometheus.Counter
	PhonesFound      prometheus.Counter
}

// NewMetrics creates and registers Prometheus metrics.
func NewMetrics() *Metrics {
	m := &Metrics{
		QueriesProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "serp_queries_processed_total",
			Help: "Total queries processed by SERP stage",
		}),
		SERPURLsFound: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "serp_urls_found_total",
			Help: "Total URLs found from SERP results",
		}),
		DomainsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "serp_domains_processed_total",
			Help: "Total domains processed by website stage",
		}),
		PagesQueued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "serp_pages_queued_total",
			Help: "Total pages queued for contact extraction",
		}),
		PagesProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "serp_pages_processed_total",
			Help: "Total pages processed by contact stage",
		}),
		EmailsFound: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "serp_emails_found_total",
			Help: "Total emails extracted",
		}),
		PhonesFound: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "serp_phones_found_total",
			Help: "Total phones extracted",
		}),
	}

	prometheus.MustRegister(
		m.QueriesProcessed, m.SERPURLsFound,
		m.DomainsProcessed, m.PagesQueued,
		m.PagesProcessed, m.EmailsFound, m.PhonesFound,
	)
	return m
}

// Handler returns the Prometheus HTTP handler.
func Handler() http.Handler {
	return promhttp.Handler()
}
