package vparquet2

import (
	"bytes"
	"encoding/json"
	"sort"

	"github.com/grafana/tempo/tempodb/encoding/common"
)

type index struct {
	lastID    common.ID
	RowGroups []common.ID `json:"rowGroups"`
}

func (i *index) Add(id common.ID) {
	i.lastID = id
}

func (i *index) Flush() {
	i.RowGroups = append(i.RowGroups, i.lastID)
}

func (i *index) Marshal() ([]byte, error) {
	return json.Marshal(i)
}

func (i *index) Find(id common.ID) (ok bool, rowGroup int) {
	n := sort.Search(len(i.RowGroups), func(j int) bool {
		return bytes.Compare(id, i.RowGroups[j]) <= 0
	})
	if n >= len(i.RowGroups) {
		// Beyond the last row group. This is the only
		// area where presence can be ruled out.
		return false, -1
	}
	return true, n
}

func unmarshalIndex(b []byte) (*index, error) {
	i := &index{}
	return i, json.Unmarshal(b, i)
}