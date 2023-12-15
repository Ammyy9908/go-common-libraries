package logger

import (
	"context"
	"errors"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LogLevel int

const (
	Debug LogLevel = iota
	Info
	Warn
	Error
	Fatal
)

const (
	CorrelationID            = "X-Correlation-ID"
	errCorrelationIDNotFound = "correlation id was not found in context"
)

// New returns a new zap.SugaredLogger instance with predefined encoding
func New(minLogLevel LogLevel) *zap.SugaredLogger {
	return zap.New(getCore(minLogLevel), zap.AddCaller()).Sugar()
}

// NonSugaredLogger returns a new zap.Logger instance with predefined encoding
func NonSugaredLogger(minLogLevel LogLevel) *zap.Logger {
	return zap.New(getCore(minLogLevel), zap.AddCaller())
}

// WithCorrelation returns a zap.SugaredLogger instance with Correlation ID as a field taken from the context and predefined encoding
func WithCorrelation(ctx context.Context, minLogLevel LogLevel) (*zap.SugaredLogger, error) {
	core := getCore(minLogLevel)
	newLogger := zap.New(core, zap.AddCaller()).Sugar()
	ctxCorrelationID, ok := ctx.Value(CorrelationID).(string)
	if !ok {
		return zap.New(core, zap.AddCaller()).Sugar(), errors.New(errCorrelationIDNotFound)
	}
	newLogger = newLogger.With(zap.String(CorrelationID, ctxCorrelationID))
	return newLogger, nil
}

// AddCorrelation adds correlation from context to existing sugaredLogger if correlation ID exists
func AddCorrelation(ctx context.Context, logger *zap.SugaredLogger) *zap.SugaredLogger {
	ctxCorrelationID, ok := ctx.Value(CorrelationID).(string)
	if !ok {
		logger.Error(errCorrelationIDNotFound)
		return logger
	}
	logger = logger.With(zap.String(CorrelationID, ctxCorrelationID))
	return logger
}

// getCore returns a zapcore.Core with a log level based on project configuration
func getCore(logLevel LogLevel) zapcore.Core {
	return zapcore.NewCore(getEncoder(), zapcore.AddSync(os.Stdout), getLogLevel(logLevel))
}

// getEncoder returns JSON encoder with set configurations
func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	return zapcore.NewJSONEncoder(encoderConfig)
}

// getLogLevel Returns zapcore.Level log level type according to set log level in viper configuration
func getLogLevel(logLevel LogLevel) zapcore.Level {
	var zapLogLevel zapcore.Level
	switch logLevel {
	// zapcore does not support Trace level
	case Debug:
		zapLogLevel = zapcore.DebugLevel
	case Info:
		zapLogLevel = zapcore.InfoLevel
	case Warn:
		zapLogLevel = zapcore.WarnLevel
	case Error:
		zapLogLevel = zapcore.ErrorLevel
	case Fatal:
		zapLogLevel = zapcore.FatalLevel
	default:
		zapLogLevel = zapcore.InfoLevel
	}
	return zapLogLevel
}
