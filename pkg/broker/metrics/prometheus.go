package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	timings = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "method_timing",
			Help:       "Per method timing",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"method"},
	)
)

func init() {
	prometheus.MustRegister(timings)
}

func TimeTrackingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		next.ServeHTTP(w, r)
		handlerName := r.URL.Path
		timings.
			WithLabelValues(handlerName).
			Observe(float64(time.Since(start).Seconds()))
	})
}
