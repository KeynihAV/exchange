package logging

import "go.uber.org/zap"

var (
	defaultLogger *zap.SugaredLogger
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
