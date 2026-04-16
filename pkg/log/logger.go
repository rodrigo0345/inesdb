package log

import (
	"fmt"
	"os"
	"strings"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

var currentLevel = LevelWarn

func init() {
	levelStr := strings.ToUpper(os.Getenv("INESDB_LOG_LEVEL"))
	if levelStr != "" {
		switch levelStr {
		case "DEBUG":
			currentLevel = LevelDebug
		case "INFO":
			currentLevel = LevelInfo
		case "WARN":
			currentLevel = LevelWarn
		case "ERROR":
			currentLevel = LevelError
		default:
			panic("Specified level unknown.")
		}
	}
}

func SetLevel(level Level) {
	currentLevel = level
}

func GetLevel() Level {
	return currentLevel
}

func shouldLog(level Level) bool {
	return level >= currentLevel
}

func Debug(format string, args ...interface{}) {
	if !shouldLog(LevelDebug) {
		return
	}
	fmt.Fprintf(os.Stderr, "[DEBUG] "+format+"\n", args...)
}

func Info(format string, args ...interface{}) {
	if !shouldLog(LevelInfo) {
		return
	}
	fmt.Fprintf(os.Stderr, "[INFO] "+format+"\n", args...)
}
func Warn(format string, args ...interface{}) {
	if !shouldLog(LevelWarn) {
		return
	}
  	fmt.Fprintf(os.Stderr, "[WARN] "+format+"\n", args...)
}

func Error(format string, args ...interface{}) {
	if !shouldLog(LevelError) {
		return
	}
	fmt.Fprintf(os.Stderr, "[ERROR] "+format+"\n", args...)
}

func IsDebugEnabled() bool {
	return shouldLog(LevelDebug)
}

func IsInfoEnabled() bool {
	return shouldLog(LevelInfo)
}

func IsWarnEnabled() bool {
	return shouldLog(LevelWarn)
}
