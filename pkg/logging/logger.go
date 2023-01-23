package logging

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
)

var (
	defaultLogger *zap.SugaredLogger
)

const (
	loggerKey    = "logger"
	requestIDKey = "reqID"
)

type Logger struct {
	Zap *zap.Logger
}

func New() *Logger {
	zapLogger, _ := zap.NewProduction()

	logger := &Logger{Zap: zapLogger}

	defaultLogger = zapLogger.With(
		zap.String("logger", "defaultLogger"),
	).WithOptions(
		zap.AddCallerSkip(1),
		zap.AddStacktrace(zap.DebugLevel),
	).Sugar()

	return logger
}

func Sl(ctx context.Context) *zap.SugaredLogger {
	if ctx == nil {
		return defaultLogger
	}
	zap, ok := ctx.Value(loggerKey).(*zap.SugaredLogger)
	if !ok || zap == nil {
		return defaultLogger
	}
	return zap
}

func (myLogger *Logger) AddReqID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID == "" {
			res := make([]byte, 16)
			rand.Read(res)
			requestID = fmt.Sprintf("%x", res)
		}
		ctx := context.WithValue(r.Context(), requestIDKey, requestID)
		r = r.WithContext(ctx)
		next.ServeHTTP(w, r)
	})
}

func (myLogger *Logger) WriteAccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)

		Sl(r.Context()).Infow("access log",
			"url", r.URL.Path,
			"method", r.Method,
			"duration", time.Since(start),
		)
	})
}

func (myLogger *Logger) SetupLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		reqID, ok := r.Context().Value(requestIDKey).(string)
		if !ok {
			reqID = "-"
		}

		ctxLogger := myLogger.Zap.With(
			zap.String("logger", "ctxlog"),
			zap.String("trace-id", reqID),
		).WithOptions(
			zap.IncreaseLevel(zap.DebugLevel),
			zap.AddCaller(),
		).Sugar()

		ctx := context.WithValue(r.Context(), loggerKey, ctxLogger)
		r = r.WithContext(ctx)

		next.ServeHTTP(w, r)
	})
}
