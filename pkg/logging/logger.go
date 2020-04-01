// Package logging provides a Logger to write logs.
package logging

import (
	"fmt"
	"log"
	"os"
)

// Logging levels.
const (
	LevelDebug = 0
	LevelInfo  = 1
	LevelWarn  = 2
	LevelError = 3
)

// Logger formats and writes logs to stderr.
type Logger struct {
	logger *log.Logger
	level  int
}

// New creates a Logger with the given logging level.
func New(level int) *Logger {
	return &Logger{
		logger: log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds|log.LUTC|log.Lshortfile),
		level:  level,
	}
}

// Level returns the logging level.
func (l *Logger) Level() int {
	return l.level
}

// Debug writes a formatted debug log.
func (l *Logger) Debug(format string, operands ...interface{}) {
	if l.level <= LevelDebug {
		l.write("DEBUG", format, operands...)
	}
}

// Info writes a formatted info log.
func (l *Logger) Info(format string, operands ...interface{}) {
	if l.level <= LevelInfo {
		l.write("INFO", format, operands...)
	}
}

// Warn writes a formatted warn log.
func (l *Logger) Warn(format string, operands ...interface{}) {
	if l.level <= LevelWarn {
		l.write("WARN", format, operands...)
	}
}

// Error writes a formatted error log.
func (l *Logger) Error(format string, operands ...interface{}) {
	if l.level <= LevelError {
		l.write("ERROR", format, operands...)
	}
}

// write writes a formatted log.
func (l *Logger) write(typ string, format string, operands ...interface{}) {
	fullFormat := fmt.Sprintf("[%v] %v", typ, format)
	l.logger.Output(3, fmt.Sprintf(fullFormat, operands...))
}
