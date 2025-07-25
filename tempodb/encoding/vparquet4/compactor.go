package vparquet4

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	tempoUtil "github.com/grafana/tempo/pkg/util"
	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/otel/attribute"

	tempo_io "github.com/grafana/tempo/pkg/io"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

func NewCompactor(opts common.CompactionOptions) *Compactor {
	return &Compactor{opts: opts}
}

type Compactor struct {
	opts common.CompactionOptions
}

// pool is a global rowPool to reuse parquet.Row buffers while compacting.
// This setup has been experimentally found to have much better performance and resource usage
// in a high volume multitenant setup.  But this isn't necessarily the best possible and could use
// more investigation. Findings:
// (1) Previously we initialized the pool with a setting based on the tenant's MaxBytesPerTrace.
// But this led to unstable memory usage, with high memory spikes and OOMs. Removing pooling altogether
// reduced memory usage by over half.
// (2) However some amount of pooling is needed to keep GC's low and compaction throughput high. 1M is
// large enough for most traces but doesn't attempt to pool for large traces (we don't repool slices that
// weren't created by the pool).
// (3) Using a global pool is an additional small but worthwhile improvement over a pool per compaction, and possible since
// it is no longer based on the tenant's MaxBytesPerTrace.
var pool = newRowPool(1_000_000)

func (c *Compactor) Compact(ctx context.Context, l log.Logger, r backend.Reader, w backend.Writer, inputs []*backend.BlockMeta) (newCompactedBlocks []*backend.BlockMeta, err error) {
	var (
		compactionLevel uint32
		totalRecords    int64
		minBlockStart   time.Time
		maxBlockEnd     time.Time
		bookmarks       = make([]*bookmark[parquet.Row], 0, len(inputs))
	)
	for _, blockMeta := range inputs {
		totalRecords += blockMeta.TotalObjects

		if blockMeta.CompactionLevel > compactionLevel {
			compactionLevel = blockMeta.CompactionLevel
		}

		if blockMeta.StartTime.Before(minBlockStart) || minBlockStart.IsZero() {
			minBlockStart = blockMeta.StartTime
		}
		if blockMeta.EndTime.After(maxBlockEnd) {
			maxBlockEnd = blockMeta.EndTime
		}

		block := newBackendBlock(blockMeta, r)

		derivedCtx, span := tracer.Start(ctx, "vparquet.compactor.iterator")
		defer span.End()

		iter, err := block.rawIter(derivedCtx, pool)
		if err != nil {
			return nil, fmt.Errorf("error creating iterator for block %s: %w", blockMeta.BlockID.String(), err)
		}

		bookmarks = append(bookmarks, newBookmark[parquet.Row](iter))
	}

	var (
		replicationFactor   = inputs[0].ReplicationFactor
		nextCompactionLevel = compactionLevel + 1
		sch                 = parquet.SchemaOf(new(Trace))
	)

	// Dedupe rows and also call the metrics callback.
	combine := func(rows []parquet.Row) (parquet.Row, error) {
		if len(rows) == 0 {
			return nil, nil
		}

		if len(rows) == 1 {
			return rows[0], nil
		}

		isEqual := true
		for i := 1; i < len(rows) && isEqual; i++ {
			isEqual = rows[0].Equal(rows[i])
		}
		if isEqual {
			for i := 1; i < len(rows); i++ {
				pool.Put(rows[i])
			}
			return rows[0], nil
		}

		// Total
		if c.opts.MaxBytesPerTrace > 0 {
			sum := 0
			for _, row := range rows {
				sum += estimateProtoSizeFromParquetRow(row)
			}
			if sum > c.opts.MaxBytesPerTrace {
				// Trace too large to compact
				for i := 1; i < len(rows); i++ {
					c.opts.SpansDiscarded(countSpans(sch, rows[i]))
					pool.Put(rows[i])
				}
				return rows[0], nil
			}
		}

		// Time to combine.
		cmb := NewCombiner()
		dedupedSpans := 0
		for i, row := range rows {
			tr := new(Trace)
			err := sch.Reconstruct(tr, row)
			if err != nil {
				return nil, err
			}
			dedupedSpans += cmb.ConsumeWithFinal(tr, i == len(rows)-1)
			pool.Put(row)
		}
		c.opts.DedupedSpans(int(replicationFactor), dedupedSpans)
		tr, _, connected := cmb.Result()
		if !connected {
			c.opts.DisconnectedTrace()
		}
		if tr != nil && tr.RootSpanName == "" {
			c.opts.RootlessTrace()
		}

		c.opts.ObjectsCombined(int(compactionLevel), 1)
		return sch.Deconstruct(pool.Get(), tr), nil
	}

	var (
		m               = newMultiblockIterator(bookmarks, combine)
		recordsPerBlock = (totalRecords / int64(c.opts.OutputBlocks))
		currentBlock    *streamingBlock
	)
	defer m.Close()

	for {
		lowestID, lowestObject, err := m.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("error iterating input blocks: %w", err)
		}

		if c.opts.DropObject != nil && c.opts.DropObject(lowestID) {
			continue
		}

		// make a new block if necessary
		if currentBlock == nil {
			// Start with a copy and then customize
			newMeta := &backend.BlockMeta{
				BlockID:           backend.NewUUID(),
				TenantID:          inputs[0].TenantID,
				CompactionLevel:   nextCompactionLevel,
				TotalObjects:      recordsPerBlock, // Just an estimate
				ReplicationFactor: replicationFactor,
				DedicatedColumns:  inputs[0].DedicatedColumns,
			}

			currentBlock = newStreamingBlock(ctx, &c.opts.BlockConfig, newMeta, r, w, tempo_io.NewBufferedWriter)
			currentBlock.meta.CompactionLevel = nextCompactionLevel
			newCompactedBlocks = append(newCompactedBlocks, currentBlock.meta)
		}

		// Flush existing block data if the next trace can't fit
		if currentBlock.EstimatedBufferedBytes() > 0 && currentBlock.EstimatedBufferedBytes()+estimateMarshalledSizeFromParquetRow(lowestObject) > c.opts.BlockConfig.RowGroupSizeBytes {
			runtime.GC()
			err = c.appendBlock(ctx, currentBlock, l)
			if err != nil {
				return nil, fmt.Errorf("error writing partial block: %w", err)
			}
		}

		// Write trace.
		// Note - not specifying trace start/end here, we set the overall block start/stop
		// times from the input metas.
		err = currentBlock.AddRaw(lowestID, lowestObject, 0, 0)
		if err != nil {
			return nil, err
		}

		// Flush again if block is already full.
		if currentBlock.EstimatedBufferedBytes() > c.opts.BlockConfig.RowGroupSizeBytes {
			runtime.GC()
			err = c.appendBlock(ctx, currentBlock, l)
			if err != nil {
				return nil, fmt.Errorf("error writing partial block: %w", err)
			}
		}

		pool.Put(lowestObject)

		// ship block to backend if done
		if currentBlock.meta.TotalObjects >= recordsPerBlock {
			currentBlockPtrCopy := currentBlock
			currentBlockPtrCopy.meta.StartTime = minBlockStart
			currentBlockPtrCopy.meta.EndTime = maxBlockEnd
			err := c.finishBlock(ctx, currentBlockPtrCopy, l)
			if err != nil {
				return nil, fmt.Errorf("error shipping block to backend, blockID %s: %w", currentBlockPtrCopy.meta.BlockID.String(), err)
			}
			currentBlock = nil
		}
	}

	// ship final block to backend
	if currentBlock != nil {
		currentBlock.meta.StartTime = minBlockStart
		currentBlock.meta.EndTime = maxBlockEnd
		err := c.finishBlock(ctx, currentBlock, l)
		if err != nil {
			return nil, fmt.Errorf("error shipping block to backend, blockID %s: %w", currentBlock.meta.BlockID.String(), err)
		}
	}

	return newCompactedBlocks, nil
}

func (c *Compactor) appendBlock(ctx context.Context, block *streamingBlock, l log.Logger) error {
	_, span := tracer.Start(ctx, "vparquet.compactor.appendBlock")
	defer span.End()

	var (
		objs            = block.CurrentBufferedObjects()
		vals            = block.EstimatedBufferedBytes()
		compactionLevel = int(block.meta.CompactionLevel - 1)
	)

	if c.opts.ObjectsWritten != nil {
		c.opts.ObjectsWritten(compactionLevel, objs)
	}

	bytesFlushed, err := block.Flush()
	if err != nil {
		return err
	}

	if c.opts.BytesWritten != nil {
		c.opts.BytesWritten(compactionLevel, bytesFlushed)
	}

	level.Info(l).Log("msg", "flushed to block", "bytes", bytesFlushed, "objects", objs, "values", vals)

	return nil
}

func (c *Compactor) finishBlock(ctx context.Context, block *streamingBlock, l log.Logger) error {
	_, span := tracer.Start(ctx, "vparquet.compactor.finishBlock")
	defer span.End()

	bytesFlushed, err := block.Complete()
	if err != nil {
		return fmt.Errorf("error completing block: %w", err)
	}

	level.Info(l).Log("msg", "wrote compacted block",
		"version", block.meta.Version,
		"tenantID", block.meta.TenantID,
		"blockID", block.meta.BlockID.String(),
		"startTime", block.meta.StartTime.String(),
		"endTime", block.meta.EndTime.String(),
		"totalObjects", block.meta.TotalObjects,
		"size", block.meta.Size_,
		"compactionLevel", block.meta.CompactionLevel,
		"encoding", block.meta.Encoding.String(),
		"totalRecords", block.meta.TotalObjects,
		"bloomShardCount", block.meta.BloomShardCount,
		"footerSize", block.meta.FooterSize,
		"replicationFactor", block.meta.ReplicationFactor,
		"dedicatedColumns", fmt.Sprintf("%+v", block.meta.DedicatedColumns),
	)

	span.AddEvent("wrote compacted block")
	span.SetAttributes(
		attribute.String("blockID", block.meta.BlockID.String()),
	)

	compactionLevel := int(block.meta.CompactionLevel) - 1
	if c.opts.BytesWritten != nil {
		c.opts.BytesWritten(compactionLevel, bytesFlushed)
	}
	return nil
}

type rowPool struct {
	pool sync.Pool
	size int
}

func newRowPool(defaultRowSize int) *rowPool {
	return &rowPool{
		size: defaultRowSize,
		pool: sync.Pool{
			New: func() any {
				return make(parquet.Row, 0, defaultRowSize)
			},
		},
	}
}

func (r *rowPool) Get() parquet.Row {
	return r.pool.Get().(parquet.Row)
}

func (r *rowPool) Put(row parquet.Row) {
	if cap(row) != r.size {
		// Dont' repool slices that weren't created by this pool.
		return
	}
	// Clear before putting into the pool.
	// This is important so that pool entries don't hang
	// onto the underlying buffers.
	clear(row)
	r.pool.Put(row[:0]) //nolint:all //SA6002
}

// estimateProtoSizeFromParquetRow estimates the byte-length of the corresponding
// trace in tempopb.Trace format. This method is unreasonably effective.
// Testing on real blocks shows 90-98% accuracy.
func estimateProtoSizeFromParquetRow(row parquet.Row) (size int) {
	for _, v := range row {
		size++ // Field identifier

		switch v.Kind() {
		case parquet.ByteArray:
			size += len(v.ByteArray())

		case parquet.FixedLenByteArray:
			size += len(v.ByteArray())

		default:
			// All other types (ints, bools) approach 1 byte per value
			size++
		}
	}
	return
}

// estimateMarshalledSizeFromParquetRow estimates the byte size as marshalled into parquet.
// this is a very rough estimate and is generally 66%-100% of actual size.
func estimateMarshalledSizeFromParquetRow(row parquet.Row) (size int) {
	for _, v := range row {
		sz := 1

		switch v.Kind() {
		case parquet.ByteArray:
			sz = len(v.ByteArray())

		case parquet.FixedLenByteArray:
			sz = len(v.ByteArray())
		}

		if sz > 1 {
			// for larger values, estimate 1 byte per 20 bytes. this is a very rough estimate
			// but works well enough for our purposes. TODO: improve this calculation
			sz = max(sz/20, 1) // nolint: typecheck
		}

		size += sz
	}
	return
}

// countSpans counts the number of spans in the given trace in deconstructed
// parquet row format and returns traceId.
// It simply counts the number of values for span ID, which is always present.
func countSpans(schema *parquet.Schema, row parquet.Row) (traceID, rootSpanName, rootServiceName string, spans int) {
	traceIDColumn, found := schema.Lookup(TraceIDColumnName)
	if !found {
		return "", "", "", 0
	}

	rootSpanNameColumn, found := schema.Lookup(columnPathRootSpanName)
	if !found {
		return "", "", "", 0
	}

	rootServiceNameColumn, found := schema.Lookup(columnPathRootServiceName)
	if !found {
		return "", "", "", 0
	}

	spanID, found := schema.Lookup("rs", "list", "element", "ss", "list", "element", "Spans", "list", "element", "SpanID")
	if !found {
		return "", "", "", 0
	}

	for _, v := range row {
		if v.Column() == spanID.ColumnIndex {
			spans++
		}

		if v.Column() == traceIDColumn.ColumnIndex {
			traceID = tempoUtil.TraceIDToHexString(v.ByteArray())
		}

		if v.Column() == rootSpanNameColumn.ColumnIndex {
			rootSpanName = v.String()
		}

		if v.Column() == rootServiceNameColumn.ColumnIndex {
			rootServiceName = v.String()
		}
	}

	return
}
