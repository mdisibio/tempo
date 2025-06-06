package inspect

import (
	"fmt"

	"github.com/grafana/tempo/pkg/parquetinspect/output"
	"github.com/parquet-go/parquet-go"
)

type DumpLine struct {
	RowNumber *int
	Line      []*parquet.Value
}

func (d *DumpLine) Cells() []any {
	cells := make([]any, 0, len(d.Line)+1)
	if d.RowNumber == nil {
		cells = append(cells, "")
	} else {
		cells = append(cells, *d.RowNumber)
	}

	for _, v := range d.Line {
		if v == nil {
			cells = append(cells, "")
		} else if v.IsNull() {
			cells = append(cells, "null")
		} else {
			cells = append(cells, v.String())
		}
	}
	return cells
}

type RowDumpOptions struct {
	Pagination
	Columns []int
}

func NewRowDump(file *parquet.File, options RowDumpOptions) (*RowDump, error) {
	all := LeafColumns(file)
	var columns []*parquet.Column

	if len(options.Columns) == 0 {
		columns = all
	} else {
		columns = make([]*parquet.Column, 0, len(options.Columns))
		for _, idx := range options.Columns {
			if idx >= len(all) {
				return nil, fmt.Errorf("column index expectd be below %d but was %d", idx, len(all))
			}
			columns = append(columns, all[idx])
		}
	}

	c := RowDump{
		header:     make([]any, 0, len(columns)+1),
		columnIter: make([]*groupingColumnIterator, 0, len(columns)),
		row: rowValues{
			values: make([][]parquet.Value, len(columns)),
		},
	}

	c.header = append(c.header, "Row")
	for _, col := range columns {
		it, err := newGroupingColumnIterator(col, nil, options.Pagination)
		if err != nil {
			return nil, fmt.Errorf("unable to create row stats calculator: %w", err)
		}
		c.columnIter = append(c.columnIter, it)
		c.header = append(c.header, col.Name())
	}

	return &c, nil
}

type rowValues struct {
	values [][]parquet.Value
	index  int
}

type RowDump struct {
	header     []any
	columnIter []*groupingColumnIterator
	rowNumber  int
	row        rowValues
}

func (rd *RowDump) Header() []any {
	return rd.header
}

func (rd *RowDump) NextRow() (output.TableRow, error) {
	if !rd.hasUnreadRowValues() {
		err := rd.readRowValues()
		if err != nil {
			return nil, err
		}
	}

	dl := DumpLine{Line: make([]*parquet.Value, 0, len(rd.row.values))}
	if rd.row.index == 0 {
		dl.RowNumber = &rd.rowNumber
	}

	for _, vals := range rd.row.values {
		if rd.row.index < len(vals) {
			dl.Line = append(dl.Line, &vals[rd.row.index])
		} else {
			dl.Line = append(dl.Line, nil)
		}
	}
	rd.row.index++

	return &dl, nil
}

func (rd *RowDump) hasUnreadRowValues() bool {
	for _, vals := range rd.row.values {
		if rd.row.index < len(vals) {
			return true
		}
	}
	return false
}

func (rd *RowDump) readRowValues() error {
	for i, it := range rd.columnIter {
		vals, err := it.NextGroup()
		if err != nil {
			return err
		}
		rd.row.values[i] = vals
	}
	rd.rowNumber++
	rd.row.index = 0
	return nil
}
