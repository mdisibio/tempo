package vparquet5

import (
	v1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	"github.com/grafana/tempo/tempodb/backend"
)

// Column paths for spare dedicated attribute columns
var DedicatedResourceColumnPaths = map[backend.DedicatedColumnScope]map[backend.DedicatedColumnType][]string{
	backend.DedicatedColumnScopeResource: {
		backend.DedicatedColumnTypeString: {
			"rs.list.element.Resource.DedicatedAttributes.String01",
			"rs.list.element.Resource.DedicatedAttributes.String02",
			"rs.list.element.Resource.DedicatedAttributes.String03",
			"rs.list.element.Resource.DedicatedAttributes.String04",
			"rs.list.element.Resource.DedicatedAttributes.String05",
			"rs.list.element.Resource.DedicatedAttributes.String06",
			"rs.list.element.Resource.DedicatedAttributes.String07",
			"rs.list.element.Resource.DedicatedAttributes.String08",
			"rs.list.element.Resource.DedicatedAttributes.String09",
			"rs.list.element.Resource.DedicatedAttributes.String10",
			"rs.list.element.Resource.DedicatedAttributes.String11",
			"rs.list.element.Resource.DedicatedAttributes.String12",
			"rs.list.element.Resource.DedicatedAttributes.String13",
			"rs.list.element.Resource.DedicatedAttributes.String14",
			"rs.list.element.Resource.DedicatedAttributes.String15",
			"rs.list.element.Resource.DedicatedAttributes.String16",
			"rs.list.element.Resource.DedicatedAttributes.String17",
			"rs.list.element.Resource.DedicatedAttributes.String18",
			"rs.list.element.Resource.DedicatedAttributes.String19",
			"rs.list.element.Resource.DedicatedAttributes.String20",
		},
	},
	backend.DedicatedColumnScopeSpan: {
		backend.DedicatedColumnTypeString: {
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String01",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String02",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String03",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String04",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String05",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String06",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String07",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String08",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String09",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String10",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String11",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String12",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String13",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String14",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String15",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String16",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String17",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String18",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String19",
			"rs.list.element.ss.list.element.Spans.list.element.DedicatedAttributes.String20",
		},
	},
	backend.DedicatedColumnScopeEvent: {
		backend.DedicatedColumnTypeString: {
			"rs.list.element.ss.list.element.Spans.list.element.Events.list.element.DedicatedAttributes.String01",
			"rs.list.element.ss.list.element.Spans.list.element.Events.list.element.DedicatedAttributes.String02",
			"rs.list.element.ss.list.element.Spans.list.element.Events.list.element.DedicatedAttributes.String03",
			"rs.list.element.ss.list.element.Spans.list.element.Events.list.element.DedicatedAttributes.String04",
			"rs.list.element.ss.list.element.Spans.list.element.Events.list.element.DedicatedAttributes.String05",
		},
	},
}

type dedicatedColumn struct {
	Type        backend.DedicatedColumnType
	Flags       uint8
	ColumnPath  string
	ColumnIndex int
}

func (dc dedicatedColumn) Blob() bool {
	return dc.Flags&uint8(backend.DedicatedColumnFlagBlob) != 0
}

func (attrs DedicatedAttributes5) readValue(dc dedicatedColumn) *v1.AnyValue {
	switch dc.Type {
	case backend.DedicatedColumnTypeString:
		var strVal *string
		switch dc.ColumnIndex {
		case 0:
			strVal = attrs.String01
		case 1:
			strVal = attrs.String02
		case 2:
			strVal = attrs.String03
		case 3:
			strVal = attrs.String04
		case 4:
			strVal = attrs.String05
		}

		if strVal != nil {
			return &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: *strVal}}
		}
	}

	return nil
}

func (attrs DedicatedAttributes20) readValue(dc dedicatedColumn) *v1.AnyValue {
	switch dc.Type {
	case backend.DedicatedColumnTypeString:
		var strVal *string
		switch dc.ColumnIndex {
		case 0:
			strVal = attrs.String01
		case 1:
			strVal = attrs.String02
		case 2:
			strVal = attrs.String03
		case 3:
			strVal = attrs.String04
		case 4:
			strVal = attrs.String05
		case 5:
			strVal = attrs.String06
		case 6:
			strVal = attrs.String07
		case 7:
			strVal = attrs.String08
		case 8:
			strVal = attrs.String09
		case 9:
			strVal = attrs.String10
		case 10:
			strVal = attrs.String11
		case 11:
			strVal = attrs.String12
		case 12:
			strVal = attrs.String13
		case 13:
			strVal = attrs.String14
		case 14:
			strVal = attrs.String15
		case 15:
			strVal = attrs.String16
		case 16:
			strVal = attrs.String17
		case 17:
			strVal = attrs.String18
		case 18:
			strVal = attrs.String19
		case 19:
			strVal = attrs.String20

		}
		if strVal == nil {
			return nil
		}
		return &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: *strVal}}
	default:
		return nil
	}
}

func (attrs *DedicatedAttributesSpan) readValue(dc dedicatedColumn) *v1.AnyValue {
	switch dc.Type {
	case backend.DedicatedColumnTypeString:
		return attrs.DedicatedAttributes20.readValue(dc)
	}
	return nil
}

func (attrs *DedicatedAttributesEvent) readValue(dc dedicatedColumn) *v1.AnyValue {
	switch dc.Type {
	case backend.DedicatedColumnTypeString:
		return attrs.DedicatedAttributes5.readValue(dc)
	}
	return nil
}

func (attrs *DedicatedAttributes5) writeValue(dc dedicatedColumn, value *v1.AnyValue) bool {
	switch dc.Type {
	case backend.DedicatedColumnTypeString:
		strVal, ok := value.Value.(*v1.AnyValue_StringValue)
		if !ok {
			return false
		}
		switch dc.ColumnIndex {
		case 0:
			attrs.String01 = &strVal.StringValue
			return true
		case 1:
			attrs.String02 = &strVal.StringValue
			return true
		case 2:
			attrs.String03 = &strVal.StringValue
			return true
		case 3:
			attrs.String04 = &strVal.StringValue
			return true
		case 4:
			attrs.String05 = &strVal.StringValue
			return true
		}
	}
	return false
}

func (attrs *DedicatedAttributes20) writeValue(dc dedicatedColumn, value *v1.AnyValue) bool {
	switch dc.Type {
	case backend.DedicatedColumnTypeString:
		strVal, ok := value.Value.(*v1.AnyValue_StringValue)
		if !ok {
			return false
		}
		switch dc.ColumnIndex {
		case 0:
			attrs.String01 = &strVal.StringValue
			return true
		case 1:
			attrs.String02 = &strVal.StringValue
			return true
		case 2:
			attrs.String03 = &strVal.StringValue
			return true
		case 3:
			attrs.String04 = &strVal.StringValue
			return true
		case 4:
			attrs.String05 = &strVal.StringValue
			return true
		case 5:
			attrs.String06 = &strVal.StringValue
			return true
		case 6:
			attrs.String07 = &strVal.StringValue
			return true
		case 7:
			attrs.String08 = &strVal.StringValue
			return true
		case 8:
			attrs.String09 = &strVal.StringValue
			return true
		case 9:
			attrs.String10 = &strVal.StringValue
			return true
		case 10:
			attrs.String11 = &strVal.StringValue
			return true
		case 11:
			attrs.String12 = &strVal.StringValue
			return true
		case 12:
			attrs.String13 = &strVal.StringValue
			return true
		case 13:
			attrs.String14 = &strVal.StringValue
			return true
		case 14:
			attrs.String15 = &strVal.StringValue
			return true
		case 15:
			attrs.String16 = &strVal.StringValue
			return true
		case 16:
			attrs.String17 = &strVal.StringValue
			return true
		case 17:
			attrs.String18 = &strVal.StringValue
			return true
		case 18:
			attrs.String19 = &strVal.StringValue
			return true
		case 19:
			attrs.String20 = &strVal.StringValue
			return true
		}
	}
	return false
}

func (attrs *DedicatedAttributesSpan) writeValue(dc dedicatedColumn, value *v1.AnyValue) bool {
	switch dc.Type {
	case backend.DedicatedColumnTypeString:
		return attrs.DedicatedAttributes20.writeValue(dc, value)
	}
	return false
}

func (attrs *DedicatedAttributesEvent) writeValue(dc dedicatedColumn, value *v1.AnyValue) bool {
	switch dc.Type {
	case backend.DedicatedColumnTypeString:
		return attrs.DedicatedAttributes5.writeValue(dc, value)
	}
	return false
}

func newDedicatedColumnMapping(size int) dedicatedColumnMapping {
	return dedicatedColumnMapping{
		mapping: make(map[string]dedicatedColumn, size),
		keys:    make([]string, 0, size),
	}
}

// dedicatedColumnMapping maps the attribute names to dedicated columns while preserving the
// order of dedicated columns
type dedicatedColumnMapping struct {
	mapping map[string]dedicatedColumn
	keys    []string
}

func (dm *dedicatedColumnMapping) put(attr string, col dedicatedColumn) {
	dm.mapping[attr] = col
	dm.keys = append(dm.keys, attr)
}

func (dm *dedicatedColumnMapping) get(attr string) (dedicatedColumn, bool) {
	col, ok := dm.mapping[attr]
	return col, ok
}

func (dm *dedicatedColumnMapping) forEach(callback func(attr string, column dedicatedColumn)) {
	for _, k := range dm.keys {
		callback(k, dm.mapping[k])
	}
}

func (dm *dedicatedColumnMapping) Blob(index int) bool {
	if index >= 0 && index < len(dm.keys) {
		return dm.mapping[dm.keys[index]].Blob()
	}
	return false
}

var allScopes = []backend.DedicatedColumnScope{backend.DedicatedColumnScopeResource, backend.DedicatedColumnScopeSpan}

// dedicatedColumnsToColumnMapping returns mapping from attribute names to spare columns for a give
// block meta and scope.
func dedicatedColumnsToColumnMapping(dedicatedColumns backend.DedicatedColumns, scopes ...backend.DedicatedColumnScope) dedicatedColumnMapping {
	if len(scopes) == 0 {
		scopes = allScopes
	}

	mapping := newDedicatedColumnMapping(len(dedicatedColumns))

	for _, scope := range scopes {
		spareColumnsByType, ok := DedicatedResourceColumnPaths[scope]
		if !ok {
			continue
		}

		indexByType := map[backend.DedicatedColumnType]int{}
		for _, c := range dedicatedColumns {
			if c.Scope != scope {
				continue
			}
			spareColumnPaths, exists := spareColumnsByType[c.Type]
			if !exists {
				continue
			}

			i := indexByType[c.Type]
			if i >= len(spareColumnPaths) {
				continue // skip if there are not enough spare columns
			}

			mapping.put(c.Name, dedicatedColumn{
				Type:        c.Type,
				Flags:       c.Flags,
				ColumnPath:  spareColumnPaths[i],
				ColumnIndex: i,
			})
			indexByType[c.Type]++
		}
	}

	return mapping
}
