package main

import (
	"io"
	"log"
	"os"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

type Logger struct {
	level int
	std   *log.Logger
}

const (
	lvlDebug = 10
	lvlInfo  = 20
	lvlWarn  = 30
	lvlError = 40
)

func parseLevel(s string) int {
	switch strings.ToUpper(s) {
	case "DEBUG":
		return lvlDebug
	case "INFO":
		return lvlInfo
	case "WARN", "WARNING":
		return lvlWarn
	case "ERROR":
		return lvlError
	}
	return lvlInfo
}

func NewLogger(file, level string) *Logger {
	var w io.Writer = os.Stdout
	if file != "" {
		w = &lumberjack.Logger{
			Filename:   file,
			MaxSize:    1,
			MaxBackups: 3,
			MaxAge:     14,
			Compress:   true,
		}
	}
	return &Logger{
		level: parseLevel(level),
		std:   log.New(w, "", log.LstdFlags|log.Lmicroseconds),
	}
}

func (l *Logger) emit(lvl int, tag, format string, args ...interface{}) {
	if lvl < l.level {
		return
	}
	l.std.Printf(tag+" "+format, args...)
}

func (l *Logger) Debug(format string, args ...interface{}) { l.emit(lvlDebug, "[DEBUG]", format, args...) }
func (l *Logger) Info(format string, args ...interface{})  { l.emit(lvlInfo, "[INFO]", format, args...) }
func (l *Logger) Warn(format string, args ...interface{})  { l.emit(lvlWarn, "[WARN]", format, args...) }
func (l *Logger) Error(format string, args ...interface{}) { l.emit(lvlError, "[ERROR]", format, args...) }
