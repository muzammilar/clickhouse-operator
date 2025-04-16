package util

import (
	"context"

	"github.com/go-logr/logr"
	"go.uber.org/zap"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
)

type Logger interface {
	Named(name string) Logger
	WithContext(ctx context.Context, object client.Object) Logger
	With(keysAndVals ...interface{}) Logger

	Debug(msg string, keysAndVals ...interface{})
	Info(msg string, keysAndVals ...interface{})
	Warn(msg string, keysAndVals ...interface{})
	Error(err error, msg string, keysAndVals ...interface{})
	Fatal(err error, msg string, keysAndVals ...interface{})
	Panic(err error, msg string, keysAndVals ...interface{})
}

type ZapLogger struct {
	l *zap.Logger
}

var _ Logger = &ZapLogger{}

func NewZapLogger(l *zap.Logger) Logger {
	return &ZapLogger{l: l}
}

func (z *ZapLogger) Named(name string) Logger {
	return &ZapLogger{l: z.l.Named(name)}
}

func (z *ZapLogger) WithContext(ctx context.Context, object client.Object) Logger {
	return &ZapLogger{l: z.l.With(
		zap.String("reconcile_id", string(controller.ReconcileIDFromContext(ctx))),
		zap.String("namespace", object.GetNamespace()),
		zap.String("name", object.GetName()),
	)}
}

func (z *ZapLogger) With(keysAndVals ...interface{}) Logger {
	return &ZapLogger{
		l: z.l.With(z.toFields(keysAndVals...)...),
	}
}

func (z *ZapLogger) Debug(msg string, keysAndVals ...interface{}) {
	z.l.Debug(msg, z.toFields(keysAndVals...)...)
}

func (z *ZapLogger) Info(msg string, keysAndVals ...interface{}) {
	z.l.Info(msg, z.toFields(keysAndVals...)...)
}

func (z *ZapLogger) Warn(msg string, keysAndVals ...interface{}) {
	z.l.Warn(msg, z.toFields(keysAndVals...)...)
}

func (z *ZapLogger) Error(err error, msg string, keysAndVals ...interface{}) {
	z.l.Error(msg, append(z.toFields(keysAndVals...), zap.String("error", err.Error()))...)
}

func (z *ZapLogger) Fatal(err error, msg string, keysAndVals ...interface{}) {
	z.l.Fatal(msg, append(z.toFields(keysAndVals...), zap.String("error", err.Error()))...)
}

func (z *ZapLogger) Panic(err error, msg string, keysAndVals ...interface{}) {
	z.l.Panic(msg, append(z.toFields(keysAndVals...), zap.String("error", err.Error()))...)
}

func (z *ZapLogger) toFields(args ...interface{}) []zap.Field {
	if len(args)%2 != 0 {
		z.l.Fatal("odd number of keys and values", zap.Any("keys", args))
		return nil
	}

	result := make([]zap.Field, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			z.l.Fatal("non string key found", zap.Any("key", args[i*2]))
			return nil
		}

		// Handle types that implement logr.Marshaler: log the replacement
		// object instead of the original one.
		val := args[i+1]
		if marshaler, ok := val.(logr.Marshaler); ok {
			val = marshaler.MarshalLog()
		}
		result = append(result, zap.Any(key, val))
	}

	return result
}
