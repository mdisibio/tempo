package combiner

import (
	"fmt"

	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/model/trace"
	"github.com/grafana/tempo/pkg/tempopb"
)

func NewTypedTraceByIDV2(maxBytes int, marshalingFormat api.MarshallingFormat, traceRedactor TraceRedactor) GRPCCombiner[*tempopb.TraceByIDResponse] {
	return NewTraceByIDV2(maxBytes, marshalingFormat, traceRedactor).(GRPCCombiner[*tempopb.TraceByIDResponse])
}

func NewTraceByIDV2(maxBytes int, marshalingFormat api.MarshallingFormat, traceRedactor TraceRedactor) Combiner {
	var (
		partialTrace    bool
		finalized       bool
		combiner        = trace.NewCombiner(maxBytes, true)
		metricsCombiner = NewTraceByIDMetricsCombiner()
	)

	gc := &genericCombiner[*tempopb.TraceByIDResponse]{
		combine: func(partial *tempopb.TraceByIDResponse, _ *tempopb.TraceByIDResponse, pipelineResp PipelineResponse) error {
			// If we are finalized and already returning a response
			// then don't modify anything.
			// This is more efficient than cloning the response before returning it.
			if finalized {
				return nil
			}

			if partial.Status == tempopb.PartialStatus_PARTIAL {
				partialTrace = true
			}

			metricsCombiner.Combine(partial.Metrics, pipelineResp)

			_, err := combiner.Consume(partial.Trace)
			return err
		},
		finalize: func(resp *tempopb.TraceByIDResponse) (*tempopb.TraceByIDResponse, error) {
			traceResult, _ := combiner.Result()
			if traceResult == nil {
				traceResult = &tempopb.Trace{}
			}

			// dedupe duplicate span ids
			deduper := newDeduper()
			traceResult = deduper.dedupe(traceResult)
			if traceRedactor != nil {
				err := traceRedactor.RedactTraceAttributes(traceResult)
				if err != nil {
					return nil, err
				}
			}
			resp.Trace = traceResult
			resp.Metrics = metricsCombiner.Metrics

			if partialTrace || combiner.IsPartialTrace() {
				resp.Status = tempopb.PartialStatus_PARTIAL
				resp.Message = fmt.Sprintf("Trace exceeds maximum size of %d bytes, a partial trace is returned", maxBytes)
			}

			// There is an issue here where after we return the proto response here,
			// more partial results could be received (due to jobs not cancelling immediately).
			// To prevent that we signal that we are finalized and accept no more responses after.
			finalized = true
			return resp, nil
		},
		new:     func() *tempopb.TraceByIDResponse { return &tempopb.TraceByIDResponse{} },
		current: &tempopb.TraceByIDResponse{},
	}
	initHTTPCombiner(gc, marshalingFormat)
	return gc
}
