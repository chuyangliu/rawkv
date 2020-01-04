package logging

import (
	"fmt"
	"log"
	"os"
)

// Logger formats and writes log messages
type Logger struct {
	logger *log.Logger
}

// New instantiates a Logger
func New() *Logger {
	return &Logger{
		logger: log.New(os.Stderr, "", log.LstdFlags|log.LUTC|log.Lshortfile),
	}
}

// Debug writes formatted log with debug messages
func (l *Logger) Debug(format string, operands ...interface{}) {
	l.write("DEBUG", format, operands...)
}

// Info writes formatted log with information messages
func (l *Logger) Info(format string, operands ...interface{}) {
	l.write("INFO", format, operands...)
}

// Warn writes formatted log with warning messages
func (l *Logger) Warn(format string, operands ...interface{}) {
	l.write("WARN", format, operands...)
}

// Error writes formatted log with error messages
func (l *Logger) Error(format string, operands ...interface{}) {
	l.write("ERROR", format, operands...)
}

func (l *Logger) write(typ string, format string, operands ...interface{}) {
	fullFormat := fmt.Sprintf("[%v] %v", typ, format)
	l.logger.Output(2, fmt.Sprintf(fullFormat, operands...))
}
