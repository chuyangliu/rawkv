package logging

import (
	"fmt"
	"log"
	"os"
)

// Candidate values for logging level.
const (
	LevelDebug = iota
	LevelInfo  = iota
	LevelWarn  = iota
	LevelError = iota
)

// Logger formats and writes log messages.
type Logger struct {
	logger *log.Logger
	level  int
}

// New instantiates a Logger.
func New(level int) *Logger {
	return &Logger{
		logger: log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds|log.LUTC|log.Lshortfile),
		level:  level,
	}
}

// Debug writes formatted log with debug messages.
func (l *Logger) Debug(format string, operands ...interface{}) {
	if l.level <= LevelDebug {
		l.write("DEBUG", format, operands...)
	}
}

// Info writes formatted log with information messages.
func (l *Logger) Info(format string, operands ...interface{}) {
	if l.level <= LevelInfo {
		l.write("INFO", format, operands...)
	}
}

// Warn writes formatted log with warning messages.
func (l *Logger) Warn(format string, operands ...interface{}) {
	if l.level <= LevelWarn {
		l.write("WARN", format, operands...)
	}
}

// Error writes formatted log with error messages.
func (l *Logger) Error(format string, operands ...interface{}) {
	if l.level <= LevelError {
		l.write("ERROR", format, operands...)
	}
}

func (l *Logger) write(typ string, format string, operands ...interface{}) {
	fullFormat := fmt.Sprintf("[%v] %v", typ, format)
	l.logger.Output(3, fmt.Sprintf(fullFormat, operands...))
}
