package utils

import (
	"log/slog"
	"os"
)

type SlogWrapper struct {
	logger *slog.Logger
}

func (w *SlogWrapper) Debug(msg string, args ...any) {
	w.logger.Debug(msg, args)
}

func (w *SlogWrapper) Info(msg string, args ...any) {
	w.logger.Info(msg, args)
}

func (w *SlogWrapper) Warn(msg string, args ...any) {
	w.logger.Warn(msg, args)
}

func (w *SlogWrapper) Error(args ...any) {
	w.logger.Error("Error", args)
}

func NewSlogWrapper(logger *slog.Logger) *SlogWrapper {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return &SlogWrapper{
		logger: logger,
	}
}
