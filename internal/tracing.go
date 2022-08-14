package internal

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	gactor "github.com/blong14/gache/internal/actors"
	genv "github.com/blong14/gache/internal/environment"
)

func Trace(
	ctx context.Context,
	tracer trace.Tracer,
	query *gactor.Query,
	name string,
) (context.Context, trace.Span) {
	span := trace.SpanFromContext(ctx)
	if genv.TraceEnabled() {
		ctx, span = tracer.Start(ctx, name)
		defer span.End()
		span.SetAttributes(
			attribute.String(
				"query-instruction",
				query.Header.Inst.String(),
			),
		)
	}
	return ctx, span
}
