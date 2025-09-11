package vparquet5

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/dataquality"
	tempo_io "github.com/grafana/tempo/pkg/io"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/parquet-go/parquet-go"
)

type backendWriter struct {
	ctx      context.Context
	w        backend.Writer
	name     string
	blockID  uuid.UUID
	tenantID string
	tracker  backend.AppendTracker
}

var _ io.WriteCloser = (*backendWriter)(nil)

func (b *backendWriter) Write(p []byte) (n int, err error) {
	b.tracker, err = b.w.Append(b.ctx, b.name, b.blockID, b.tenantID, b.tracker, p)
	return len(p), err
}

func (b *backendWriter) Close() error {
	return b.w.CloseAppend(b.ctx, b.tracker)
}

func CreateBlock(ctx context.Context, cfg *common.BlockConfig, meta *backend.BlockMeta, i common.Iterator, r backend.Reader, to backend.Writer) (*backend.BlockMeta, error) {
	s := newStreamingBlock(ctx, cfg, meta, r, to, tempo_io.NewBufferedWriter)

	var next func(context.Context) error

	if ii, ok := i.(*commonIterator); ok {
		next = func(ctx context.Context) error {
			// Use interal iterator and avoid translation to/from proto
			id, row, err := ii.NextRow(ctx)
			if err != nil {
				return err
			}
			if row == nil {
				return io.EOF
			}
			err = s.AddRaw(id, row, 0, 0) // start and end time of the wal meta are used.
			if err != nil {
				return err
			}

			completeBlockRowPool.Put(row)
			return nil
		}
	} else {
		// Need to convert from proto->parquet obj
		var (
			buffer       = &Trace{}
			connected    bool
			resMapping   = dedicatedColumnsToColumnMapping(meta.DedicatedColumns, backend.DedicatedColumnScopeResource)
			spanMapping  = dedicatedColumnsToColumnMapping(meta.DedicatedColumns, backend.DedicatedColumnScopeSpan)
			eventMapping = dedicatedColumnsToColumnMapping(meta.DedicatedColumns, backend.DedicatedColumnScopeEvent)
		)
		next = func(context.Context) error {
			id, tr, err := i.Next(ctx)
			if err != nil {
				return err
			}
			if tr == nil {
				return io.EOF
			}

			// Copy ID to allow it to escape the iterator.
			id = append([]byte(nil), id...)

			buffer, connected = traceToParquetWithMapping(id, tr, buffer, resMapping, spanMapping, eventMapping)
			if !connected {
				dataquality.WarnDisconnectedTrace(meta.TenantID, dataquality.PhaseTraceWalToComplete)
			}
			if buffer.RootSpanName == "" {
				dataquality.WarnRootlessTrace(meta.TenantID, dataquality.PhaseTraceWalToComplete)
			}

			err = s.Add(buffer, 0, 0) // start and end time are set outside
			if err != nil {
				return err
			}

			return nil
		}
	}

	for {
		err := next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		if s.EstimatedBufferedBytes() > cfg.RowGroupSizeBytes {
			_, err = s.Flush()
			if err != nil {
				return nil, err
			}
			runtime.GC()
		}
	}

	_, err := s.Complete()
	if err != nil {
		return nil, err
	}

	return s.meta, nil
}

type streamingBlock struct {
	ctx   context.Context
	bloom *common.ShardedBloomFilter
	meta  *backend.BlockMeta
	bw    tempo_io.BufferedWriteFlusher
	pw    *parquet.GenericWriter[*Trace]
	w     *backendWriter
	r     backend.Reader
	to    backend.Writer
	index *index

	withNoCompactFlag bool

	currentBufferedTraces int
	currentBufferedBytes  int
}

func newStreamingBlock(ctx context.Context, cfg *common.BlockConfig, meta *backend.BlockMeta, r backend.Reader, to backend.Writer, createBufferedWriter func(w io.Writer) tempo_io.BufferedWriteFlusher) *streamingBlock {
	newMeta := backend.NewBlockMetaWithDedicatedColumns(meta.TenantID, (uuid.UUID)(meta.BlockID), VersionString, backend.EncNone, "", meta.DedicatedColumns)
	newMeta.StartTime = meta.StartTime
	newMeta.EndTime = meta.EndTime
	newMeta.ReplicationFactor = meta.ReplicationFactor

	// TotalObjects is used here an an estimated count for the bloom filter.
	// The real number of objects is tracked below.
	bloom := common.NewBloom(cfg.BloomFP, uint(cfg.BloomShardSizeBytes), uint(meta.TotalObjects))

	w := &backendWriter{ctx, to, DataFileName, (uuid.UUID)(meta.BlockID), meta.TenantID, nil}
	bw := createBufferedWriter(w)

	var (
		resMapping   = dedicatedColumnsToColumnMapping(meta.DedicatedColumns, backend.DedicatedColumnScopeResource)
		spanMapping  = dedicatedColumnsToColumnMapping(meta.DedicatedColumns, backend.DedicatedColumnScopeSpan)
		eventMapping = dedicatedColumnsToColumnMapping(meta.DedicatedColumns, backend.DedicatedColumnScopeEvent)
	)

	fieldTagsCallback := func(typ reflect.Type, f *reflect.StructField, tag string) (replacement string) {
		// Determine scope based on parent type.
		var dm *dedicatedColumnMapping
		switch typ {
		case reflect.TypeOf(DedicatedAttributesSpan{}):
			dm = &spanMapping
		case reflect.TypeOf(DedicatedAttributes20{}):
			dm = &resMapping
		case reflect.TypeOf(DedicatedAttributesEvent{}):
			dm = &eventMapping
		default:
			return tag
		}

		// Parse dedicated column index out of the name.
		// String01 is index 0 below.
		indexStr, ok := strings.CutPrefix(f.Name, "String")
		if !ok {
			return tag
		}
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return tag
		}

		if dm.Blob(index - 1) {
			return ",zstd,optional"
		}

		return tag
	}

	schema := parquet.SchemaOf(&Trace{}, parquet.FieldTagsCallbackOption(fieldTagsCallback))

	options := []parquet.WriterOption{
		schema,
	}

	// Minor optimization: skip page bounds for blob columns. The min/max values are not
	// selective, and this saves a little bit of storage and overhead.
	for key, col := range spanMapping.mapping {
		if col.Blob() {
			fmt.Println("Blob column:", col.ColumnPath, key)
			options = append(options, parquet.SkipPageBounds(strings.Split(col.ColumnPath, ".")...))
		}
	}
	for key, col := range resMapping.mapping {
		if col.Blob() {
			fmt.Println("Blob column:", col.ColumnPath, key)
			options = append(options, parquet.SkipPageBounds(strings.Split(col.ColumnPath, ".")...))
		}
	}
	for key, col := range eventMapping.mapping {
		if col.Blob() {
			fmt.Println("Blob column:", col.ColumnPath, key)
			options = append(options, parquet.SkipPageBounds(strings.Split(col.ColumnPath, ".")...))
		}
	}

	pw := parquet.NewGenericWriter[*Trace](bw, options...)

	return &streamingBlock{
		ctx:               ctx,
		meta:              newMeta,
		bloom:             bloom,
		bw:                bw,
		pw:                pw,
		w:                 w,
		r:                 r,
		to:                to,
		index:             &index{},
		withNoCompactFlag: cfg.CreateWithNoCompactFlag,
	}
}

func (b *streamingBlock) Add(tr *Trace, start, end uint32) error {
	_, err := b.pw.Write([]*Trace{tr})
	if err != nil {
		return err
	}
	id := tr.TraceID

	b.index.Add(id)
	b.bloom.Add(id)
	b.meta.ObjectAdded(start, end)
	b.currentBufferedTraces++
	b.currentBufferedBytes += estimateMarshalledSizeFromTrace(tr)

	return nil
}

func (b *streamingBlock) AddRaw(id []byte, row parquet.Row, start, end uint32) error {
	_, err := b.pw.WriteRows([]parquet.Row{row})
	if err != nil {
		return err
	}

	b.index.Add(id)
	b.bloom.Add(id)
	b.meta.ObjectAdded(start, end)
	b.currentBufferedTraces++
	b.currentBufferedBytes += estimateMarshalledSizeFromParquetRow(row)

	return nil
}

func (b *streamingBlock) EstimatedBufferedBytes() int {
	return b.currentBufferedBytes
}

func (b *streamingBlock) CurrentBufferedObjects() int {
	return b.currentBufferedTraces
}

func (b *streamingBlock) Flush() (int, error) {
	// Flush row group
	b.index.Flush()
	err := b.pw.Flush()
	if err != nil {
		return 0, err
	}

	n := b.bw.Len()
	b.meta.Size_ += uint64(n)
	b.meta.TotalRecords++
	b.currentBufferedTraces = 0
	b.currentBufferedBytes = 0

	// Flush to underlying writer
	return n, b.bw.Flush()
}

func (b *streamingBlock) Complete() (int, error) {
	// Flush final row group
	b.index.Flush()
	b.meta.TotalRecords++
	err := b.pw.Flush()
	if err != nil {
		return 0, err
	}

	// Close parquet file. This writes the footer and metadata.
	err = b.pw.Close()
	if err != nil {
		return 0, err
	}

	// Now Flush and close out in-memory buffer
	n := b.bw.Len()
	b.meta.Size_ += uint64(n)
	err = b.bw.Flush()
	if err != nil {
		return 0, err
	}

	err = b.bw.Close()
	if err != nil {
		return 0, err
	}

	err = b.w.Close()
	if err != nil {
		return 0, err
	}

	// Read the footer size out of the parquet footer
	buf := make([]byte, 8)
	err = b.r.ReadRange(b.ctx, DataFileName, (uuid.UUID)(b.meta.BlockID), b.meta.TenantID, b.meta.Size_-8, buf, nil)
	if err != nil {
		return 0, fmt.Errorf("error reading parquet file footer: %w", err)
	}
	if string(buf[4:8]) != "PAR1" {
		return 0, errors.New("failed to confirm magic footer while writing a new parquet block")
	}
	b.meta.FooterSize = binary.LittleEndian.Uint32(buf[0:4])

	b.meta.BloomShardCount = uint32(b.bloom.GetShardCount())

	if b.withNoCompactFlag {
		// write nocompact flag first to prevent compaction before completion
		err := b.to.WriteNoCompactFlag(b.ctx, (uuid.UUID)(b.meta.BlockID), b.meta.TenantID)
		if err != nil {
			return 0, fmt.Errorf("unexpected error writing nocompact flag: %w", err)
		}
	}

	return n, writeBlockMeta(b.ctx, b.to, b.meta, b.bloom, b.index)
}

// estimateMarshalledSizeFromTrace estimates the size of this trace when written to parquet. This is used to
// determine when to cut a row group during block creation.   This function is about 85% accurate.
func estimateMarshalledSizeFromTrace(tr *Trace) (size int) {
	size += 7 // 7 trace lvl fields
	size += len(tr.TraceID)

	for _, rs := range tr.ResourceSpans {
		size += estimateAttrSize(rs.Resource.Attrs)
		size += 21 // 21 resource lvl fields including dedicated attributes
		size += 50 // 50 dedicated columns

		for _, ils := range rs.ScopeSpans {
			size += 2 // 2 scope span lvl fields
			size += 4 // 4 scope fields
			size += estimateAttrSize(ils.Scope.Attrs)

			for _, s := range ils.Spans {
				size += 35 // 35 span lvl fields including dedicated attributes
				size += 50 // 50 dedicated columns
				size += len(s.SpanID) + len(s.ParentSpanID)
				size += estimateAttrSize(s.Attrs)
				size += estimateEventsSize(s.Events)
				size += estimateLinksSize(s.Links)
			}
		}
	}
	return
}

func estimateAttrSize(attrs []Attribute) (size int) {
	size += len(attrs) * 7 // 7 attribute lvl fields

	// 1 byte for every entry in arrays after the first one.
	for _, a := range attrs {
		size += max(0, len(a.Value)-1)
		size += max(0, len(a.ValueInt)-1)
		size += max(0, len(a.ValueDouble)-1)
		size += max(0, len(a.ValueBool)-1)
	}

	return
}

func estimateEventsSize(events []Event) (size int) {
	for _, e := range events {
		size += 4  // 4 event lvl fields
		size += 50 // 50 dedicated columns
		size += estimateAttrSize(e.Attrs)
	}
	return
}

func estimateLinksSize(links []Link) (size int) {
	for _, l := range links {
		size += 5 // 5 link lvl fields
		size += len(l.TraceID) + len(l.SpanID)
		size += estimateAttrSize(l.Attrs)
	}
	return
}
