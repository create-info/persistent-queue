package queue

import (
	"context"
	"fmt"
)

type Logger interface {
	GetNewContext(serviceName string) context.Context
	Info(ctx context.Context, info string)
	Warn(ctx context.Context, warning string)
	Error(ctx context.Context, error string)
	Debug(ctx context.Context, info string)
}

type Log struct {
	L Logger
}

func (l Log) GetNewContext(serviceName string) context.Context {
	return l.L.GetNewContext(serviceName)
}

func (l Log) Info(ctx context.Context, format string, args ...interface{}) {
	if len(args) == 0 {
		l.L.Info(ctx, format)
	} else {
		l.L.Info(ctx, fmt.Sprintf(format, args...))
	}
}

func (l Log) Warn(ctx context.Context, format string, args ...interface{}) {
	if len(args) == 0 {
		l.L.Warn(ctx, format)
	} else {
		l.L.Warn(ctx, fmt.Sprintf(format, args...))
	}
}

func (l Log) Error(ctx context.Context, format string, args ...interface{}) {
	if len(args) == 0 {
		l.L.Error(ctx, format)
	} else {
		l.L.Error(ctx, fmt.Sprintf(format, args...))
	}
}

func (l Log) Debug(ctx context.Context, format string, args ...interface{}) {
	if len(args) == 0 {
		l.L.Debug(ctx, format)
	} else {
		l.L.Debug(ctx, fmt.Sprintf(format, args...))
	}
}
