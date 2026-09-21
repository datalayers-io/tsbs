// Package logging provides a logrus-based logger that prints a full
// timestamp and the caller source location on every log line.
package logging

import (
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/sirupsen/logrus"
)

const timestampFormat = "2006-01-02 15:04:05"

// Logger wraps logrus so that every entry carries a full timestamp and a
// source field pointing at the caller's file and line.
type Logger struct {
	*logrus.Logger
}

// New creates a logger that writes to stderr with full timestamps enabled.
func New() *Logger {
	l := logrus.New()
	l.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: timestampFormat,
	})
	return &Logger{Logger: l}
}

// source attaches a field identifying the caller file and line, mirroring
// the historical "source=file:line" field of the prometheus logger.
func (l *Logger) source() *logrus.Entry {
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		return l.WithField("source", "<unknown>")
	}
	return l.WithField("source", fmt.Sprintf("%s:%d", filepath.Base(file), line))
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	l.source().Debugf(format, args...)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.source().Infof(format, args...)
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	l.source().Warnf(format, args...)
}

func (l *Logger) Error(args ...interface{}) {
	l.source().Error(args...)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.source().Errorf(format, args...)
}
