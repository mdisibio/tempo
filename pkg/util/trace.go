package util

import (
	"bytes"
	"hash"
	"hash/fnv"

	"github.com/pkg/errors"
	"github.com/richardartoul/molecule"
	"github.com/richardartoul/molecule/src/codec"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/tempo/pkg/tempopb"
	v1_common "github.com/open-telemetry/opentelemetry-proto/gen/go/common/v1"
	v1_resource "github.com/open-telemetry/opentelemetry-proto/gen/go/resource/v1"
	v1 "github.com/open-telemetry/opentelemetry-proto/gen/go/trace/v1"
)

const FieldNum_Trace_Batches = 1
const FieldNum_ResourceSpans_Resource = 1
const FieldNum_ResourceSpans_ILS = 2
const FieldNum_ILS_IL = 1
const FieldNum_ILS_Spans = 2
const FieldNum_Span_SpanID = 2

func CombineTracesMolecule(objA []byte, objB []byte) ([]byte, error) {

	traceA := &tempopb.Trace{}
	_ = proto.Unmarshal(objA, traceA)

	traceB := &tempopb.Trace{}
	_ = proto.Unmarshal(objB, traceB)

	h := fnv.New32()

	spansInA := make(map[uint32]struct{})
	for _, batchA := range traceA.Batches {
		for _, ilsA := range batchA.InstrumentationLibrarySpans {
			for _, spanA := range ilsA.Spans {
				spansInA[tokenForID(h, spanA.SpanId)] = struct{}{}
			}
			//spanCountA += len(ilsA.Spans)
			//spanCountTotal += len(ilsA.Spans)
		}
	}

	//var ilsToAdd []v1.InstrumentationLibrarySpans

	//traceA.Batches[0].InstrumentationLibrarySpans[0].Spans[0].SpanId
	//       Field 1    Field 2                        Field 2  Field 2
	var err error
	buf := codec.NewBuffer(objB)
	err = molecule.MessageEach(buf, func(fieldNum int32, value molecule.Value) (bool, error) {
		if fieldNum == FieldNum_Trace_Batches {
			batchBytes, _ := value.AsBytesUnsafe()

			var ilsToAdd []*v1.InstrumentationLibrarySpans
			var lastResource []byte
			err = molecule.MessageEach(codec.NewBuffer(batchBytes), func(fieldNum int32, value molecule.Value) (bool, error) {
				// value is v1.ResourceSpans
				switch fieldNum {
				case FieldNum_ResourceSpans_Resource:
					lastResource, _ = value.AsBytesUnsafe()

				case FieldNum_ResourceSpans_ILS:
					ilsBytes, _ := value.AsBytesUnsafe()

					var spansToAdd []*v1.Span
					var lastIL []byte
					err = molecule.MessageEach(codec.NewBuffer(ilsBytes), func(fieldNum int32, value molecule.Value) (bool, error) {
						if fieldNum == FieldNum_ILS_IL {
							// Instrumentation Library
							lastIL, err = value.AsBytesUnsafe()
							if err != nil {
								return false, err
							}
						}
						if fieldNum == FieldNum_ILS_Spans {
							spanBytes, _ := value.AsBytesUnsafe()

							err = molecule.MessageEach(codec.NewBuffer(spanBytes), func(fieldNum int32, value molecule.Value) (bool, error) {
								if fieldNum == FieldNum_Span_SpanID {
									spanID, err := value.AsBytesUnsafe()
									if err != nil {
										return false, err
									}

									tokenB := tokenForID(h, spanID)
									if _, ok := spansInA[tokenB]; !ok {
										// This span not in objA
										// Go ahead and unmarshal and save
										s := &v1.Span{}
										err = proto.Unmarshal(spanBytes, s)
										if err != nil {
											return false, errors.Wrap(err, "error unmarshaling span")
										}

										spansToAdd = append(spansToAdd, s)
									}
									// We are done scanning
									return false, nil
								}
								return true, nil
							})
							if err != nil {
								return false, err
							}
							return true, nil
						}
						return true, nil
					})
					if err != nil {
						return false, err
					}

					if spansToAdd != nil {
						ils := v1.InstrumentationLibrarySpans{
							Spans: spansToAdd,
						}
						// Deserialize preceeding fields if present
						if len(lastIL) > 0 {
							ils.InstrumentationLibrary = &v1_common.InstrumentationLibrary{}
							err := proto.Unmarshal(lastIL, ils.InstrumentationLibrary)
							if err != nil {
								return false, err
							}
						}
						ilsToAdd = append(ilsToAdd, &ils)
					}
					return true, nil
				}
				return true, nil
			})
			if err != nil {
				return false, err
			}

			if ilsToAdd != nil {
				batch := &v1.ResourceSpans{
					InstrumentationLibrarySpans: ilsToAdd,
				}
				// Deserialize preceeding fields if present
				if len(lastResource) > 0 {
					batch.Resource = &v1_resource.Resource{}
					err := proto.Unmarshal(lastResource, batch.Resource)
					if err != nil {
						return false, err
					}
				}
				traceA.Batches = append(traceA.Batches, batch)
			}

			return true, nil
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return proto.Marshal(traceA)
}

func CombineTraces(objA []byte, objB []byte) ([]byte, error) {
	// if the byte arrays are the same, we can return quickly
	if bytes.Equal(objA, objB) {
		return objA, nil
	}

	// hashes differ.  unmarshal and combine traces
	traceA := &tempopb.Trace{}
	traceB := &tempopb.Trace{}

	errA := proto.Unmarshal(objA, traceA)
	errB := proto.Unmarshal(objB, traceB)

	// if we had problems unmarshaling one or the other, return the one that marshalled successfully
	if errA != nil && errB == nil {
		return objB, errors.Wrap(errA, "error unsmarshaling objA")
	} else if errB != nil && errA == nil {
		return objA, errors.Wrap(errB, "error unsmarshaling objB")
	} else if errA != nil && errB != nil {
		// if both failed let's send back an empty trace
		level.Error(util.Logger).Log("msg", "both A and B failed to unmarshal.  returning an empty trace")
		bytes, _ := proto.Marshal(&tempopb.Trace{})
		return bytes, errors.Wrap(errA, "both A and B failed to unmarshal.  returning an empty trace")
	}

	traceComplete, _, _, _ := CombineTraceProtos(traceA, traceB)

	bytes, err := proto.Marshal(traceComplete)
	if err != nil {
		return objA, errors.Wrap(err, "marshalling the combine trace threw an error")
	}
	return bytes, nil
}

// CombineTraceProtos combines two trace protos into one.  Note that it is destructive.
//  All spans are combined into traceA.  spanCountA, B, and Total are returned for
//  logging purposes.
func CombineTraceProtos(traceA, traceB *tempopb.Trace) (*tempopb.Trace, int, int, int) {
	// if one or the other is nil just return 0 for the one that's nil and -1 for the other.  this will be a clear indication this
	// code path was taken without unnecessarily counting spans
	if traceA == nil {
		return traceB, 0, -1, -1
	}

	if traceB == nil {
		return traceA, -1, 0, -1
	}

	spanCountA := 0
	spanCountB := 0
	spanCountTotal := 0

	h := fnv.New32()

	spansInA := make(map[uint32]struct{})
	for _, batchA := range traceA.Batches {
		for _, ilsA := range batchA.InstrumentationLibrarySpans {
			for _, spanA := range ilsA.Spans {
				spansInA[tokenForID(h, spanA.SpanId)] = struct{}{}
			}
			spanCountA += len(ilsA.Spans)
			spanCountTotal += len(ilsA.Spans)
		}
	}

	// loop through every span and copy spans in B that don't exist to A
	for _, batchB := range traceB.Batches {
		notFoundILS := batchB.InstrumentationLibrarySpans[:0]

		for _, ilsB := range batchB.InstrumentationLibrarySpans {
			notFoundSpans := ilsB.Spans[:0]
			for _, spanB := range ilsB.Spans {
				// if found in A, remove from the batch
				_, ok := spansInA[tokenForID(h, spanB.SpanId)]
				if !ok {
					notFoundSpans = append(notFoundSpans, spanB)
				}
			}
			spanCountB += len(ilsB.Spans)

			if len(notFoundSpans) > 0 {
				spanCountTotal += len(notFoundSpans)
				ilsB.Spans = notFoundSpans
				notFoundILS = append(notFoundILS, ilsB)
			}
		}

		// if there were some spans not found in A, add everything left in the batch
		if len(notFoundILS) > 0 {
			batchB.InstrumentationLibrarySpans = notFoundILS
			traceA.Batches = append(traceA.Batches, batchB)
		}
	}

	return traceA, spanCountA, spanCountB, spanCountTotal
}

func tokenForID(h hash.Hash32, b []byte) uint32 {
	h.Reset()
	_, _ = h.Write(b)
	return h.Sum32()
}
