package prometheus

import (
	"github.com/NotFound1911/gmq"
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

type MiddlewareBuilder struct {
	Namespace string
	Subsystem string
	Name      string
	Help      string
}

func (m MiddlewareBuilder) Build() gmq.Middleware {
	vector := prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:      m.Name,
		Subsystem: m.Subsystem,
		Help:      m.Help,
		Objectives: map[float64]float64{
			0.5:   0.01,
			0.75:  0.01,
			0.90:  0.01,
			0.99:  0.001,
			0.999: 0.0001,
		},
	}, []string{"topic", "content"})
	prometheus.MustRegister(vector)

	return func(next gmq.HandleFunc) gmq.HandleFunc {
		return func(msg *gmq.Msg) error {
			startTime := time.Now()
			defer func() {
				duration := time.Now().Sub(startTime).Milliseconds()
				vector.WithLabelValues(msg.Topic, msg.Content).Observe(float64(duration))
			}()
			next(msg)
			return nil
		}
	}
}
