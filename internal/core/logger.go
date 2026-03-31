package core

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/google/uuid"
)

const TRACE_ID_KEY = "traceId"

func Setup(logLevel slog.Leveler) {
	l := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(l)
}

func Get(ctx context.Context) *slog.Logger {
	id := ctx.Value(TRACE_ID_KEY)
	if id == nil {
		id = uuid.NewString()
	}
	return slog.With(TRACE_ID_KEY, id)
}

func SetTraceId(ctx context.Context) context.Context {
	id := ctx.Value(TRACE_ID_KEY)
	if id == nil {
		id = uuid.NewString()

		return context.WithValue(ctx, TRACE_ID_KEY, id)
	}
	return ctx
}

func GetWithValue(ctx context.Context) (context.Context, *slog.Logger) {
	id := ctx.Value(TRACE_ID_KEY)
	if id == nil {
		id = uuid.NewString()

		return context.WithValue(ctx, TRACE_ID_KEY, id), slog.With(TRACE_ID_KEY, id)
	}
	return ctx, slog.With(TRACE_ID_KEY, id)
}

func GetWithNewId(ctx context.Context) (context.Context, *slog.Logger) {
	id := uuid.NewString()
	return context.WithValue(ctx, TRACE_ID_KEY, id), slog.With(TRACE_ID_KEY, id)
}

func GetPPByteStr(b byte) string {
	s := fmt.Sprintf("%08b", b)
	// Group as "xxxx xxxx"
	return fmt.Sprintf("0x%02X %s %s", b, s[:4], s[4:])
}

func GetPPBytesStr(bs []byte) string {
	var sb strings.Builder
	for i, b := range bs {
		fmt.Fprintf(&sb, "%s", GetPPByteStr(b))
		if i != len(bs)-1 {
			sb.WriteString(" | ")
		}
	}

	return sb.String()

}

