package delayTask

import (
	"context"
	"github.com/sirupsen/logrus"
	"keke/infrastructure/tool"
)

var logger *logrus.Logger

func GetLogger() *logrus.Logger {
	if logger != nil {
		return logger
	}

	logger = logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetReportCaller(true)

	prettyPrint := false
	if tool.GetEnv() != "pro" {
		prettyPrint = true
	}

	logger.SetFormatter(&logrus.JSONFormatter{PrettyPrint: prettyPrint, TimestampFormat: "2006-01-02 15:04:05"})
	return logger
}

func CtxLogger(ctx context.Context) *logrus.Entry {
	opIdValue := ctx.Value("opId")
	opId, _ := opIdValue.(string)

	return GetLogger().WithFields(logrus.Fields{"opId": opId})
}
