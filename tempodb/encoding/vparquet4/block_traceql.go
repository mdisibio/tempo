package vparquet4

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/parquet-go/parquet-go"

	"github.com/grafana/tempo/pkg/parquetquery"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

var (
	pqSpanPool            = parquetquery.NewResultPool(1)
	pqSpansetPool         = parquetquery.NewResultPool(1)
	pqTracePool           = parquetquery.NewResultPool(1)
	pqAttrPool            = parquetquery.NewResultPool(1)
	pqEventPool           = parquetquery.NewResultPool(1)
	pqLinkPool            = parquetquery.NewResultPool(1)
	pqInstrumentationPool = parquetquery.NewResultPool(1)
)

type attrVal struct {
	a traceql.Attribute
	s traceql.Static
}

// span implements traceql.Span
type span struct {
	spanAttrs            []attrVal
	resourceAttrs        []attrVal
	traceAttrs           []attrVal
	eventAttrs           []attrVal
	linkAttrs            []attrVal
	instrumentationAttrs []attrVal

	id                 []byte
	startTimeUnixNanos uint64
	durationNanos      uint64
	nestedSetParent    int32
	nestedSetLeft      int32
	nestedSetRight     int32

	// metadata used to track the span in the parquet file
	rowNum         parquetquery.RowNumber
	cbSpansetFinal bool
	cbSpanset      *traceql.Spanset
}

func (s *span) AllAttributes() map[traceql.Attribute]traceql.Static {
	atts := make(map[traceql.Attribute]traceql.Static, len(s.spanAttrs)+len(s.resourceAttrs)+len(s.traceAttrs)+len(s.eventAttrs)+len(s.linkAttrs))
	for _, st := range s.traceAttrs {
		if st.s.Type == traceql.TypeNil {
			continue
		}
		atts[st.a] = st.s
	}
	for _, st := range s.resourceAttrs {
		if st.s.Type == traceql.TypeNil {
			continue
		}
		atts[st.a] = st.s
	}
	for _, st := range s.spanAttrs {
		if st.s.Type == traceql.TypeNil {
			continue
		}
		atts[st.a] = st.s
	}
	for _, st := range s.eventAttrs {
		if st.s.Type == traceql.TypeNil {
			continue
		}
		atts[st.a] = st.s
	}
	for _, st := range s.linkAttrs {
		if st.s.Type == traceql.TypeNil {
			continue
		}
		atts[st.a] = st.s
	}
	for _, st := range s.instrumentationAttrs {
		if st.s.Type == traceql.TypeNil {
			continue
		}
		atts[st.a] = st.s
	}
	return atts
}

func (s *span) AllAttributesFunc(cb func(traceql.Attribute, traceql.Static)) {
	for _, a := range s.traceAttrs {
		cb(a.a, a.s)
	}
	for _, a := range s.resourceAttrs {
		cb(a.a, a.s)
	}
	for _, a := range s.instrumentationAttrs {
		cb(a.a, a.s)
	}
	for _, a := range s.spanAttrs {
		cb(a.a, a.s)
	}
}

func (s *span) AttributeFor(a traceql.Attribute) (traceql.Static, bool) {
	find := func(a traceql.Attribute, attrs []attrVal) *traceql.Static {
		if len(attrs) == 1 {
			if attrs[0].a == a {
				return &attrs[0].s
			}
			return nil
		}
		if len(attrs) == 2 {
			if attrs[0].a == a {
				return &attrs[0].s
			}
			if attrs[1].a == a {
				return &attrs[1].s
			}
			return nil
		}

		for i := range attrs {
			if attrs[i].a == a {
				return &attrs[i].s
			}
		}
		return nil
	}
	findName := func(s string, attrs []attrVal) *traceql.Static {
		if len(attrs) == 1 {
			if attrs[0].a.Name == s {
				return &attrs[0].s
			}
			return nil
		}
		if len(attrs) == 2 {
			if attrs[0].a.Name == s {
				return &attrs[0].s
			}
			if attrs[1].a.Name == s {
				return &attrs[1].s
			}
			return nil
		}

		for i := range attrs {
			if attrs[i].a.Name == s {
				return &attrs[i].s
			}
		}
		return nil
	}

	switch a.Scope {
	case traceql.AttributeScopeResource:
		if len(s.resourceAttrs) > 0 {
			if attr := find(a, s.resourceAttrs); attr != nil {
				return *attr, true
			}
		}
		return traceql.StaticNil, false
	case traceql.AttributeScopeSpan:
		if len(s.spanAttrs) > 0 {
			if attr := find(a, s.spanAttrs); attr != nil {
				return *attr, true
			}
		}
		return traceql.StaticNil, false
	case traceql.AttributeScopeEvent:
		if len(s.eventAttrs) > 0 {
			if attr := find(a, s.eventAttrs); attr != nil {
				return *attr, true
			}
		}
		return traceql.StaticNil, false
	case traceql.AttributeScopeLink:
		if len(s.linkAttrs) > 0 {
			if attr := find(a, s.linkAttrs); attr != nil {
				return *attr, true
			}
		}
		return traceql.StaticNil, false
	case traceql.AttributeScopeInstrumentation:
		if len(s.instrumentationAttrs) > 0 {
			if attr := find(a, s.instrumentationAttrs); attr != nil {
				return *attr, true
			}
		}
		return traceql.StaticNil, false
	}

	if a.Intrinsic != traceql.IntrinsicNone {
		if a.Intrinsic == traceql.IntrinsicNestedSetLeft {
			return traceql.NewStaticInt(int(s.nestedSetLeft)), true
		}
		if a.Intrinsic == traceql.IntrinsicNestedSetRight {
			return traceql.NewStaticInt(int(s.nestedSetRight)), true
		}
		if a.Intrinsic == traceql.IntrinsicNestedSetParent {
			return traceql.NewStaticInt(int(s.nestedSetParent)), true
		}

		// intrinsics are always on the span, trace, event, or link ... for now
		if attr := find(a, s.spanAttrs); attr != nil {
			return *attr, true
		}

		if attr := find(a, s.traceAttrs); attr != nil {
			return *attr, true
		}

		if attr := find(a, s.eventAttrs); attr != nil {
			return *attr, true
		}

		if attr := find(a, s.linkAttrs); attr != nil {
			return *attr, true
		}

		if attr := find(a, s.instrumentationAttrs); attr != nil {
			return *attr, true
		}
	}

	// name search in span, resource, link, and event to give precedence to span
	// we don't need to do a name search at the trace level b/c it is intrinsics only
	if len(s.spanAttrs) > 0 {
		if attr := findName(a.Name, s.spanAttrs); attr != nil {
			return *attr, true
		}
	}

	if len(s.resourceAttrs) > 0 {
		if attr := findName(a.Name, s.resourceAttrs); attr != nil {
			return *attr, true
		}
	}

	if len(s.eventAttrs) > 0 {
		if attr := findName(a.Name, s.eventAttrs); attr != nil {
			return *attr, true
		}
	}

	if len(s.linkAttrs) > 0 {
		if attr := findName(a.Name, s.linkAttrs); attr != nil {
			return *attr, true
		}
	}

	if len(s.instrumentationAttrs) > 0 {
		if attr := findName(a.Name, s.instrumentationAttrs); attr != nil {
			return *attr, true
		}
	}

	return traceql.StaticNil, false
}

func (s *span) ID() []byte {
	return s.id
}

func (s *span) StartTimeUnixNanos() uint64 {
	return s.startTimeUnixNanos
}

func (s *span) DurationNanos() uint64 {
	return s.durationNanos
}

func (s *span) DescendantOf(lhs, rhs []traceql.Span, falseForAll, invert, union bool, buffer []traceql.Span) []traceql.Span {
	if len(lhs) == 0 && len(rhs) == 0 {
		return nil
	}

	if union {
		return descendantOfUnion(lhs, rhs, invert, buffer)
	}

	// sort by nested set left. the goal is to quickly be able to find the first entry in the lhs slice that
	// potentially matches the rhs. after we find this first potential match we just check every single lhs
	// entry til the end of the slice.
	// it might be even better to clone the lhs slice. sort one by left and one by right and search the one that
	// requires less seeking after the search. this would be faster but cloning the slice would be costly in mem
	sortFn := func(i, j int) bool { return lhs[i].(*span).nestedSetLeft > lhs[j].(*span).nestedSetLeft } // sort asc b/c we are interested in lhs nestedSetLeft > rhs nestedSetLeft
	if invert {
		sortFn = func(i, j int) bool { return lhs[i].(*span).nestedSetLeft < lhs[j].(*span).nestedSetLeft } // sort desc b/c we want the inverse relationship. see descendantOf func
	}
	sort.Slice(lhs, sortFn)

	descendantOf := func(a, b *span) bool {
		if a.nestedSetLeft == 0 ||
			b.nestedSetLeft == 0 ||
			a.nestedSetRight == 0 ||
			b.nestedSetRight == 0 {
			// Spans with missing data, never a match.
			return false
		}
		return a.nestedSetLeft > b.nestedSetLeft && a.nestedSetRight < b.nestedSetRight
	}

	for _, r := range rhs {
		matches := false
		findFn := func(i int) bool { return lhs[i].(*span).nestedSetLeft <= r.(*span).nestedSetLeft }
		if invert {
			findFn = func(i int) bool { return lhs[i].(*span).nestedSetLeft >= r.(*span).nestedSetLeft }
		}

		// let's find the first index we need to bother with.
		found := sort.Search(len(lhs), findFn)
		if found == -1 { // if we are less then the entire slice we have to search the entire slice
			found = 0
		}

		for ; found < len(lhs); found++ {
			a := lhs[found].(*span)
			b := r.(*span)
			if invert {
				a, b = b, a
			}

			if descendantOf(b, a) {
				// Returns RHS
				matches = true
				break
			}
		}
		if matches && !falseForAll || // return RHS if there are any matches on the LHS
			!matches && falseForAll { // return RHS if there are no matches on the LHS
			buffer = append(buffer, r)
		}
	}

	return buffer
}

// descendantOfUnion is a special loop designed to handle union descendantOf. technically nestedSetManyManyLoop can logically do this
// but it contains a pathological case where it devolves to O(n^2) in the worst case (a trace with a single series of nested spans).
// this loop more directly handles union descendantof.
//   - iterate the rhs checking to see if it is a descendant of lhs
//   - break out of the rhs loop when the next span will be in a different branch
//   - at this point rSpan has the narrowest span (the leaf span) of the branch
//   - iterate the lhs as long as its in the same branch as rSpan checking for ancestors
//   - go back to rhs iteration and repeat until slices are exhausted
func descendantOfUnion(lhs, rhs []traceql.Span, invert bool, buffer []traceql.Span) []traceql.Span {
	// union is harder b/c we have to find all matches on both the left and rhs
	sort.Slice(lhs, func(i, j int) bool { return lhs[i].(*span).nestedSetLeft < lhs[j].(*span).nestedSetLeft })
	if unsafe.SliceData(lhs) != unsafe.SliceData(rhs) { // if these are pointing to the same slice, no reason to sort again
		sort.Slice(rhs, func(i, j int) bool { return rhs[i].(*span).nestedSetLeft < rhs[j].(*span).nestedSetLeft })
	}

	isAfter := func(a, b *span) bool {
		return b.nestedSetLeft > a.nestedSetRight
	}
	isMatch := func(a, d *span) bool {
		if d.nestedSetLeft == 0 ||
			a.nestedSetLeft == 0 ||
			d.nestedSetRight == 0 ||
			a.nestedSetRight == 0 {
			return false
		}
		return d.nestedSetLeft > a.nestedSetLeft && d.nestedSetRight < a.nestedSetRight
	}

	if invert {
		lhs, rhs = rhs, lhs
	}

	uniqueSpans := make(map[*span]struct{}) // todo: consider a reusable map, like our buffer slice
	addToBuffer := func(s *span) {
		if _, ok := uniqueSpans[s]; !ok {
			buffer = append(buffer, s)
			uniqueSpans[s] = struct{}{}
		}
	}

	lidx := 0
	ridx := 0
	for lidx < len(lhs) && ridx < len(rhs) {
		lSpan := lhs[lidx].(*span)
		rSpan := rhs[ridx].(*span)

		// rhs
		for ; ridx < len(rhs); ridx++ {
			rSpan = rhs[ridx].(*span)

			if isMatch(lSpan, rSpan) {
				addToBuffer(rSpan)
			}

			// test the next span to see if it is still in the tree of lhs. if not bail!
			if ridx+1 < len(rhs) && isAfter(rSpan, rhs[ridx+1].(*span)) {
				ridx++
				break
			}
		}

		// lhs
		// rSpan contains the narrowest span that is in tree for lhs. advance and add lhs until we're out of tree
		for ; lidx < len(lhs); lidx++ {
			lSpan = lhs[lidx].(*span)

			// advance LHS until out of tree of RHS
			if isAfter(rSpan, lSpan) {
				break
			}

			if isMatch(lSpan, rSpan) {
				addToBuffer(lSpan)

				// if rSpan is in tree of lSpan keep on keeping on
				if ridx < len(rhs) && isMatch(lSpan, rhs[ridx].(*span)) {
					break
				}
			}
		}
	}

	return buffer
}

// SiblingOf
func (s *span) SiblingOf(lhs, rhs []traceql.Span, falseForAll, union bool, buffer []traceql.Span) []traceql.Span {
	if len(lhs) == 0 && len(rhs) == 0 {
		return nil
	}

	if union {
		// union is more difficult b/c we have to find all matches on both the left and rhs
		sort.Slice(lhs, func(i, j int) bool { return lhs[i].(*span).nestedSetParent < lhs[j].(*span).nestedSetParent })
		if unsafe.SliceData(lhs) != unsafe.SliceData(rhs) { // if these are pointing to the same slice, no reason to sort again
			sort.Slice(rhs, func(i, j int) bool { return rhs[i].(*span).nestedSetParent < rhs[j].(*span).nestedSetParent })
		}

		siblingOf := func(a, b *span) bool {
			return a.nestedSetParent == b.nestedSetParent &&
				a.nestedSetParent != 0 &&
				b.nestedSetParent != 0 &&
				a != b // a span cannot be its own sibling. note that this only works due to implementation details in the engine. if we ever pipeline structural operators then we would need to use something else for identity. rownumber?
		}

		isValid := func(s *span) bool { return s.nestedSetParent != 0 }
		isAfter := func(a, b *span) bool { return b.nestedSetParent > a.nestedSetParent }

		return nestedSetManyManyLoop(lhs, rhs, isValid, siblingOf, isAfter, falseForAll, false, union, buffer)
	}

	// this is easy. we're just looking for anything on the lhs side with the same nested set parent as the rhs
	sort.Slice(lhs, func(i, j int) bool {
		return lhs[i].(*span).nestedSetParent < lhs[j].(*span).nestedSetParent
	})

	siblingOf := func(a, b *span) bool {
		return a.nestedSetParent == b.nestedSetParent &&
			a.nestedSetParent != 0 &&
			b.nestedSetParent != 0
	}

	for _, r := range rhs {
		matches := false

		if r.(*span).nestedSetParent != 0 {
			// search for nested set parent
			found := sort.Search(len(lhs), func(i int) bool {
				return lhs[i].(*span).nestedSetParent >= r.(*span).nestedSetParent
			})

			if found >= 0 && found < len(lhs) {
				matches = siblingOf(r.(*span), lhs[found].(*span))

				// if we found a match BUT this is the same span as the match we need to check the very next span (if it exists).
				// this works b/c Search method returns the first match for nestedSetParent
				if matches && r.(*span) == lhs[found].(*span) {
					matches = false
					if found+1 < len(lhs) {
						matches = siblingOf(r.(*span), lhs[found+1].(*span))
					}
				}
			}
		}

		if matches && !falseForAll || // return RHS if there are any matches on the LHS
			!matches && falseForAll { // return RHS if there are no matches on the LHS
			buffer = append(buffer, r)
		}
	}
	return buffer
}

// {} > {}
func (s *span) ChildOf(lhs, rhs []traceql.Span, falseForAll, invert, union bool, buffer []traceql.Span) []traceql.Span {
	if len(lhs) == 0 && len(rhs) == 0 {
		return nil
	}

	if union {
		childOf := func(p, c *span) bool {
			return p.nestedSetLeft == c.nestedSetParent &&
				p.nestedSetLeft != 0 &&
				c.nestedSetParent != 0
		}
		isValid := func(s *span) bool { return s.nestedSetLeft != 0 }
		isAfter := func(p, c *span) bool { return c.nestedSetParent > p.nestedSetLeft }

		// the engine will sometimes pass the same slice for both lhs and rhs. this occurs for {} > {}.
		// if lhs is the same slice as rhs we need to make a copy of the slice to sort them by different values
		if unsafe.SliceData(lhs) == unsafe.SliceData(rhs) {
			rhs = append([]traceql.Span{}, rhs...)
		}

		parents := lhs
		children := rhs
		if invert {
			parents, children = children, parents
		}

		sort.Slice(parents, func(i, j int) bool { return parents[i].(*span).nestedSetLeft < parents[j].(*span).nestedSetLeft })
		sort.Slice(children, func(i, j int) bool { return children[i].(*span).nestedSetParent < children[j].(*span).nestedSetParent })

		return nestedSetOneManyLoop(parents, children, isValid, childOf, isAfter, falseForAll, invert, union, buffer)
	}

	// we will search the LHS by either nestedSetLeft or nestedSetParent. if we are doing child we sort by nestedSetLeft
	// so we can quickly find children. if the invert flag is set we are looking for parents and so we sort appropriately
	sortFn := func(i, j int) bool { return lhs[i].(*span).nestedSetLeft < lhs[j].(*span).nestedSetLeft }
	if invert {
		sortFn = func(i, j int) bool { return lhs[i].(*span).nestedSetParent < lhs[j].(*span).nestedSetParent }
	}

	childOf := func(a, b *span) bool {
		return a.nestedSetLeft == b.nestedSetParent &&
			a.nestedSetLeft != 0 &&
			b.nestedSetParent != 0
	}

	sort.Slice(lhs, sortFn)
	for _, r := range rhs {
		findFn := func(i int) bool { return lhs[i].(*span).nestedSetLeft >= r.(*span).nestedSetParent }
		if invert {
			findFn = func(i int) bool { return lhs[i].(*span).nestedSetParent >= r.(*span).nestedSetLeft }
		}

		// search for nested set parent
		matches := false
		found := sort.Search(len(lhs), findFn)
		if found >= 0 && found < len(lhs) {
			if invert {
				matches = childOf(r.(*span), lhs[found].(*span)) // is the rhs a child of the lhs?
			} else {
				matches = childOf(lhs[found].(*span), r.(*span)) // is the lhs a child of the rhs?
			}
		}

		if matches && !falseForAll || // return RHS if there are any matches on the LHS
			!matches && falseForAll { // return RHS if there are no matches on the LHS
			buffer = append(buffer, r)
		}
	}
	return buffer
}

// nestedSetOneManyLoop runs a standard one -> many loop to calculate nested set relationships. It handles all nested set relationships except
// siblingOf and unioned descendantOf. It forward iterates the one and many slices and applies.
func nestedSetOneManyLoop(one, many []traceql.Span, isValid func(*span) bool, isMatch, isAfter func(*span, *span) bool, falseForAll, invert, union bool, buffer []traceql.Span) []traceql.Span {
	var uniqueSpans map[*span]struct{}
	if union {
		uniqueSpans = make(map[*span]struct{}) // todo: consider a reusable map, like our buffer slice
	}

	addToBuffer := func(s *span) {
		if union {
			if _, ok := uniqueSpans[s]; !ok {
				buffer = append(buffer, s)
				uniqueSpans[s] = struct{}{}
			}
		} else {
			buffer = append(buffer, s)
		}
	}

	// note the small differences between this and the !invert loop. technically we could write these both in one piece of code,
	// but this feels better for clarity
	if invert {
		manyIdx := 0
		for _, o := range one {
			oSpan := o.(*span)

			if !isValid(oSpan) {
				continue
			}

			matches := false
			for ; manyIdx < len(many); manyIdx++ {
				mSpan := many[manyIdx].(*span)

				// if the many loop is ahead of the one loop break back to allow the one loop to let it catch up
				if isAfter(oSpan, mSpan) {
					break
				}

				if isMatch(oSpan, mSpan) {
					matches = true
					if union {
						addToBuffer(mSpan)
					} else {
						break
					}
				}
			}

			if (matches && !falseForAll) || (!matches && falseForAll) {
				addToBuffer(oSpan)
			}
		}

		return buffer
	}

	// !invert
	manyIdx := 0
	for _, o := range one {
		oSpan := o.(*span)

		if !isValid(oSpan) {
			continue
		}

		matches := false
		for ; manyIdx < len(many); manyIdx++ {
			mSpan := many[manyIdx].(*span)

			// if the many loop is ahead of the one loop break back to the allow one loop to let it catch up
			if isAfter(oSpan, mSpan) {
				break
			}

			match := isMatch(oSpan, mSpan)
			if (match && !falseForAll) || (!match && falseForAll) {
				matches = true
				addToBuffer(mSpan)
			}
		}

		if matches && union {
			addToBuffer(oSpan)
		}
	}

	// drain the rest of the children if falseForAll
	if falseForAll {
		for ; manyIdx < len(many); manyIdx++ {
			addToBuffer(many[manyIdx].(*span))
		}
	}

	return buffer
}

// nestedSetManyManyLoop handles the generic case when the lhs must be checked multiple times for each rhs. it is currently only
// used for siblingOf
func nestedSetManyManyLoop(lhs, rhs []traceql.Span, isValid func(*span) bool, isMatch, isAfter func(*span, *span) bool, falseForAll, invert, union bool, buffer []traceql.Span) []traceql.Span {
	var uniqueSpans map[*span]struct{}
	if union {
		uniqueSpans = make(map[*span]struct{}) // todo: consider a reusable map, like our buffer slice
	}

	addToBuffer := func(s *span) {
		if union {
			if _, ok := uniqueSpans[s]; !ok {
				buffer = append(buffer, s)
				uniqueSpans[s] = struct{}{}
			}
		} else {
			buffer = append(buffer, s)
		}
	}

	rescanIdx := 0
	lidx := 0
	for _, r := range rhs {
		rSpan := r.(*span)
		if !isValid(rSpan) {
			continue
		}

		// rescan whatever amount of rhs we need to
		lidx = rescanIdx

		matches := false
		for ; lidx < len(lhs); lidx++ {
			lSpan := lhs[lidx].(*span)

			// if left is after right, swap back to right
			if isAfter(rSpan, lSpan) {
				break
			}

			// if we transition forward (trees branches or parents or whatever) store current lidx to rescan
			if isAfter(lhs[rescanIdx].(*span), lSpan) {
				rescanIdx = lidx
			}

			if (!invert && isMatch(rSpan, lSpan)) || (invert && isMatch(lSpan, rSpan)) {
				matches = true
				if union {
					addToBuffer(lSpan)
				}
			}
		}

		if (matches && !falseForAll) || (!matches && falseForAll) {
			addToBuffer(rSpan)
		}
	}

	return buffer
}

func (s *span) addSpanAttr(a traceql.Attribute, st traceql.Static) {
	s.spanAttrs = append(s.spanAttrs, attrVal{a: a, s: st})
}

func (s *span) setInstrumentationAttrs(attrs []attrVal) {
	s.instrumentationAttrs = append(s.instrumentationAttrs, attrs...)
}

func (s *span) setResourceAttrs(attrs []attrVal) {
	s.resourceAttrs = append(s.resourceAttrs, attrs...)
}

func (s *span) setTraceAttrs(attrs []attrVal) {
	s.traceAttrs = append(s.traceAttrs, attrs...)
}

func (s *span) setEventAttrs(attrs []attrVal) {
	s.eventAttrs = append(s.eventAttrs, attrs...)
}

func (s *span) setLinkAttrs(attrs []attrVal) {
	s.linkAttrs = append(s.linkAttrs, attrs...)
}

// attributesMatched counts all attributes in the map as well as metadata fields like start/end/id
func (s *span) attributesMatched() int {
	count := 0
	// todo: attributesMatced is called a lot. we could cache this count on set
	for _, st := range s.spanAttrs {
		if st.s.Type != traceql.TypeNil {
			count++
		}
	}
	for _, st := range s.resourceAttrs {
		if st.s.Type != traceql.TypeNil {
			count++
		}
	}
	for _, st := range s.traceAttrs {
		if st.s.Type != traceql.TypeNil {
			count++
		}
	}
	for _, st := range s.eventAttrs {
		if st.s.Type != traceql.TypeNil {
			count++
		}
	}
	for _, st := range s.linkAttrs {
		if st.s.Type != traceql.TypeNil {
			count++
		}
	}
	for _, st := range s.instrumentationAttrs {
		if st.s.Type != traceql.TypeNil {
			count++
		}
	}
	if s.startTimeUnixNanos != 0 {
		count++
	}
	// don't count duration nanos b/c it is added to the attributes as well as the span struct
	// if s.durationNanos != 0 {
	// 	count++
	// }
	if len(s.id) > 0 {
		count++
	}
	if s.nestedSetLeft > 0 || s.nestedSetRight > 0 || s.nestedSetParent != 0 { // nestedSetParent can be -1 meaning it is a root span
		count++
	}

	return count
}

// todo: this sync pool currently massively reduces allocations by pooling spans for certain queries.
// it currently catches spans discarded:
// - in the span collector
// - in the batch collector
// - while converting to spanmeta
// to be fully effective it needs to catch spans thrown away in the query engine. perhaps filter spans
// can return a slice of dropped and kept spansets?
var spanPool = sync.Pool{
	New: func() interface{} {
		return &span{}
	},
}

func putSpan(s *span) {
	s.id = nil
	s.startTimeUnixNanos = 0
	s.durationNanos = 0
	s.rowNum = parquetquery.EmptyRowNumber()
	s.cbSpansetFinal = false
	s.cbSpanset = nil
	s.nestedSetParent = 0
	s.nestedSetLeft = 0
	s.nestedSetRight = 0
	s.spanAttrs = s.spanAttrs[:0]
	s.resourceAttrs = s.resourceAttrs[:0]
	s.traceAttrs = s.traceAttrs[:0]
	s.eventAttrs = s.eventAttrs[:0]
	s.linkAttrs = s.linkAttrs[:0]
	s.instrumentationAttrs = s.instrumentationAttrs[:0]

	spanPool.Put(s)
}

func getSpan() *span {
	return spanPool.Get().(*span)
}

var spansetPool = sync.Pool{}

func getSpanset() *traceql.Spanset {
	ss := spansetPool.Get()
	if ss == nil {
		return &traceql.Spanset{
			ReleaseFn: putSpansetAndSpans,
		}
	}

	return ss.(*traceql.Spanset)
}

// putSpanset back into the pool.  Does not repool the spans.
func putSpanset(ss *traceql.Spanset) {
	ss.Attributes = ss.Attributes[:0]
	ss.DurationNanos = 0
	ss.RootServiceName = ""
	ss.RootSpanName = ""
	ss.Scalar = traceql.NewStaticNil()
	ss.StartTimeUnixNanos = 0
	ss.TraceID = nil
	clear(ss.ServiceStats)
	ss.Spans = ss.Spans[:0]

	spansetPool.Put(ss)
}

func putSpansetAndSpans(ss *traceql.Spanset) {
	if ss != nil {
		for _, s := range ss.Spans {
			if span, ok := s.(*span); ok {
				putSpan(span)
			}
		}
		putSpanset(ss)
	}
}

const (
	columnPathTraceID                  = "TraceID"
	columnPathStartTimeUnixNano        = "StartTimeUnixNano"
	columnPathEndTimeUnixNano          = "EndTimeUnixNano"
	columnPathDurationNanos            = "DurationNano"
	columnPathRootSpanName             = "RootSpanName"
	columnPathRootServiceName          = "RootServiceName"
	columnPathServiceStatsServiceName  = "ServiceStats.key_value.key"
	columnPathServiceStatsSpanCount    = "ServiceStats.key_value.value.SpanCount"
	columnPathServiceStatsErrorCount   = "ServiceStats.key_value.value.ErrorCount"
	columnPathResourceAttrKey          = "rs.list.element.Resource.Attrs.list.element.Key"
	columnPathResourceAttrString       = "rs.list.element.Resource.Attrs.list.element.Value.list.element"
	columnPathResourceAttrInt          = "rs.list.element.Resource.Attrs.list.element.ValueInt.list.element"
	columnPathResourceAttrDouble       = "rs.list.element.Resource.Attrs.list.element.ValueDouble.list.element"
	columnPathResourceAttrBool         = "rs.list.element.Resource.Attrs.list.element.ValueBool.list.element"
	columnPathResourceServiceName      = "rs.list.element.Resource.ServiceName"
	columnPathResourceCluster          = "rs.list.element.Resource.Cluster"
	columnPathResourceNamespace        = "rs.list.element.Resource.Namespace"
	columnPathResourcePod              = "rs.list.element.Resource.Pod"
	columnPathResourceContainer        = "rs.list.element.Resource.Container"
	columnPathResourceK8sClusterName   = "rs.list.element.Resource.K8sClusterName"
	columnPathResourceK8sNamespaceName = "rs.list.element.Resource.K8sNamespaceName"
	columnPathResourceK8sPodName       = "rs.list.element.Resource.K8sPodName"
	columnPathResourceK8sContainerName = "rs.list.element.Resource.K8sContainerName"

	columnPathInstrumentationName       = "rs.list.element.ss.list.element.Scope.Name"
	columnPathInstrumentationVersion    = "rs.list.element.ss.list.element.Scope.Version"
	columnPathInstrumentationAttrKey    = "rs.list.element.ss.list.element.Scope.Attrs.list.element.Key"
	columnPathInstrumentationAttrString = "rs.list.element.ss.list.element.Scope.Attrs.list.element.Value.list.element"
	columnPathInstrumentationAttrInt    = "rs.list.element.ss.list.element.Scope.Attrs.list.element.ValueInt.list.element"
	columnPathInstrumentationAttrDouble = "rs.list.element.ss.list.element.Scope.Attrs.list.element.ValueDouble.list.element"
	columnPathInstrumentationAttrBool   = "rs.list.element.ss.list.element.Scope.Attrs.list.element.ValueBool.list.element"

	columnPathSpanID              = "rs.list.element.ss.list.element.Spans.list.element.SpanID"
	columnPathSpanName            = "rs.list.element.ss.list.element.Spans.list.element.Name"
	columnPathSpanStartTime       = "rs.list.element.ss.list.element.Spans.list.element.StartTimeUnixNano"
	columnPathSpanDuration        = "rs.list.element.ss.list.element.Spans.list.element.DurationNano"
	columnPathSpanKind            = "rs.list.element.ss.list.element.Spans.list.element.Kind"
	columnPathSpanStatusCode      = "rs.list.element.ss.list.element.Spans.list.element.StatusCode"
	columnPathSpanStatusMessage   = "rs.list.element.ss.list.element.Spans.list.element.StatusMessage"
	columnPathSpanAttrKey         = "rs.list.element.ss.list.element.Spans.list.element.Attrs.list.element.Key"
	columnPathSpanAttrString      = "rs.list.element.ss.list.element.Spans.list.element.Attrs.list.element.Value.list.element"
	columnPathSpanAttrInt         = "rs.list.element.ss.list.element.Spans.list.element.Attrs.list.element.ValueInt.list.element"
	columnPathSpanAttrDouble      = "rs.list.element.ss.list.element.Spans.list.element.Attrs.list.element.ValueDouble.list.element"
	columnPathSpanAttrBool        = "rs.list.element.ss.list.element.Spans.list.element.Attrs.list.element.ValueBool.list.element"
	columnPathSpanHTTPStatusCode  = "rs.list.element.ss.list.element.Spans.list.element.HttpStatusCode"
	columnPathSpanHTTPMethod      = "rs.list.element.ss.list.element.Spans.list.element.HttpMethod"
	columnPathSpanHTTPURL         = "rs.list.element.ss.list.element.Spans.list.element.HttpUrl"
	columnPathSpanNestedSetLeft   = "rs.list.element.ss.list.element.Spans.list.element.NestedSetLeft"
	columnPathSpanNestedSetRight  = "rs.list.element.ss.list.element.Spans.list.element.NestedSetRight"
	columnPathSpanParentID        = "rs.list.element.ss.list.element.Spans.list.element.ParentID"
	columnPathSpanParentSpanID    = "rs.list.element.ss.list.element.Spans.list.element.ParentSpanID"
	columnPathEventName           = "rs.list.element.ss.list.element.Spans.list.element.Events.list.element.Name"
	columnPathEventTimeSinceStart = "rs.list.element.ss.list.element.Spans.list.element.Events.list.element.TimeSinceStartNano"
	columnPathLinkTraceID         = "rs.list.element.ss.list.element.Spans.list.element.Links.list.element.TraceID"
	columnPathLinkSpanID          = "rs.list.element.ss.list.element.Spans.list.element.Links.list.element.SpanID"
	columnPathEventAttrKey        = "rs.list.element.ss.list.element.Spans.list.element.Events.list.element.Attrs.list.element.Key"
	columnPathEventAttrString     = "rs.list.element.ss.list.element.Spans.list.element.Events.list.element.Attrs.list.element.Value.list.element"
	columnPathEventAttrInt        = "rs.list.element.ss.list.element.Spans.list.element.Events.list.element.Attrs.list.element.ValueInt.list.element"
	columnPathEventAttrDouble     = "rs.list.element.ss.list.element.Spans.list.element.Events.list.element.Attrs.list.element.ValueDouble.list.element"
	columnPathEventAttrBool       = "rs.list.element.ss.list.element.Spans.list.element.Events.list.element.Attrs.list.element.ValueBool.list.element"
	columnPathLinkAttrKey         = "rs.list.element.ss.list.element.Spans.list.element.Links.list.element.Attrs.list.element.Key"
	columnPathLinkAttrString      = "rs.list.element.ss.list.element.Spans.list.element.Links.list.element.Attrs.list.element.Value.list.element"
	columnPathLinkAttrInt         = "rs.list.element.ss.list.element.Spans.list.element.Links.list.element.Attrs.list.element.ValueInt.list.element"
	columnPathLinkAttrDouble      = "rs.list.element.ss.list.element.Spans.list.element.Links.list.element.Attrs.list.element.ValueDouble.list.element"
	columnPathLinkAttrBool        = "rs.list.element.ss.list.element.Spans.list.element.Links.list.element.Attrs.list.element.ValueBool.list.element"

	otherEntrySpansetKey         = "spanset"
	otherEntrySpanKey            = "span"
	otherEntryEventKey           = "event"
	otherEntryLinkKey            = "link"
	otherEntryInstrumentationKey = "instrumentation"

	// a fake intrinsic scope at the trace lvl
	intrinsicScopeTrace           = -1
	intrinsicScopeSpan            = -2
	intrinsicScopeEvent           = -3
	intrinsicScopeLink            = -4
	intrinsicScopeInstrumentation = -5
)

// todo: scope is the only field used here. either remove the other fields or use them.
var intrinsicColumnLookups = map[traceql.Intrinsic]struct {
	scope      traceql.AttributeScope
	typ        traceql.StaticType
	columnPath string
}{
	traceql.IntrinsicName:                 {intrinsicScopeSpan, traceql.TypeString, columnPathSpanName},
	traceql.IntrinsicStatus:               {intrinsicScopeSpan, traceql.TypeStatus, columnPathSpanStatusCode},
	traceql.IntrinsicStatusMessage:        {intrinsicScopeSpan, traceql.TypeString, columnPathSpanStatusMessage},
	traceql.IntrinsicDuration:             {intrinsicScopeSpan, traceql.TypeDuration, columnPathSpanDuration},
	traceql.IntrinsicKind:                 {intrinsicScopeSpan, traceql.TypeKind, columnPathSpanKind},
	traceql.IntrinsicSpanID:               {intrinsicScopeSpan, traceql.TypeString, columnPathSpanID},
	traceql.IntrinsicParentID:             {intrinsicScopeSpan, traceql.TypeString, columnPathSpanParentSpanID},
	traceql.IntrinsicSpanStartTime:        {intrinsicScopeSpan, traceql.TypeString, columnPathSpanStartTime},
	traceql.IntrinsicStructuralDescendant: {intrinsicScopeSpan, traceql.TypeNil, ""}, // Not a real column, this entry is only used to assign default scope.
	traceql.IntrinsicStructuralChild:      {intrinsicScopeSpan, traceql.TypeNil, ""}, // Not a real column, this entry is only used to assign default scope.
	traceql.IntrinsicStructuralSibling:    {intrinsicScopeSpan, traceql.TypeNil, ""}, // Not a real column, this entry is only used to assign default scope.
	traceql.IntrinsicNestedSetLeft:        {intrinsicScopeSpan, traceql.TypeInt, columnPathSpanNestedSetLeft},
	traceql.IntrinsicNestedSetRight:       {intrinsicScopeSpan, traceql.TypeInt, columnPathSpanNestedSetRight},
	traceql.IntrinsicNestedSetParent:      {intrinsicScopeSpan, traceql.TypeInt, columnPathSpanParentID},

	traceql.IntrinsicTraceRootService: {intrinsicScopeTrace, traceql.TypeString, columnPathRootServiceName},
	traceql.IntrinsicTraceRootSpan:    {intrinsicScopeTrace, traceql.TypeString, columnPathRootSpanName},
	traceql.IntrinsicTraceDuration:    {intrinsicScopeTrace, traceql.TypeString, columnPathDurationNanos},
	traceql.IntrinsicTraceID:          {intrinsicScopeTrace, traceql.TypeString, columnPathTraceID},
	traceql.IntrinsicTraceStartTime:   {intrinsicScopeTrace, traceql.TypeDuration, columnPathStartTimeUnixNano},

	traceql.IntrinsicEventName:           {intrinsicScopeEvent, traceql.TypeString, columnPathEventName},
	traceql.IntrinsicEventTimeSinceStart: {intrinsicScopeEvent, traceql.TypeDuration, columnPathEventTimeSinceStart},
	traceql.IntrinsicLinkTraceID:         {intrinsicScopeLink, traceql.TypeString, columnPathLinkTraceID},
	traceql.IntrinsicLinkSpanID:          {intrinsicScopeLink, traceql.TypeString, columnPathLinkSpanID},

	traceql.IntrinsicInstrumentationName:    {intrinsicScopeInstrumentation, traceql.TypeString, columnPathInstrumentationName},
	traceql.IntrinsicInstrumentationVersion: {intrinsicScopeInstrumentation, traceql.TypeString, columnPathInstrumentationVersion},

	traceql.IntrinsicServiceStats: {intrinsicScopeTrace, traceql.TypeNil, ""}, // Not a real column, this entry is only used to assign default scope.
}

// Lookup table of all well-known attributes with dedicated columns
var wellKnownColumnLookups = map[string]struct {
	columnPath string                 // path.to.column
	level      traceql.AttributeScope // span or resource level
	typ        traceql.StaticType     // Data type
}{
	// Resource-level columns
	LabelServiceName:      {columnPathResourceServiceName, traceql.AttributeScopeResource, traceql.TypeString},
	LabelCluster:          {columnPathResourceCluster, traceql.AttributeScopeResource, traceql.TypeString},
	LabelNamespace:        {columnPathResourceNamespace, traceql.AttributeScopeResource, traceql.TypeString},
	LabelPod:              {columnPathResourcePod, traceql.AttributeScopeResource, traceql.TypeString},
	LabelContainer:        {columnPathResourceContainer, traceql.AttributeScopeResource, traceql.TypeString},
	LabelK8sClusterName:   {columnPathResourceK8sClusterName, traceql.AttributeScopeResource, traceql.TypeString},
	LabelK8sNamespaceName: {columnPathResourceK8sNamespaceName, traceql.AttributeScopeResource, traceql.TypeString},
	LabelK8sPodName:       {columnPathResourceK8sPodName, traceql.AttributeScopeResource, traceql.TypeString},
	LabelK8sContainerName: {columnPathResourceK8sContainerName, traceql.AttributeScopeResource, traceql.TypeString},

	// Span-level columns
	LabelHTTPStatusCode: {columnPathSpanHTTPStatusCode, traceql.AttributeScopeSpan, traceql.TypeInt},
	LabelHTTPMethod:     {columnPathSpanHTTPMethod, traceql.AttributeScopeSpan, traceql.TypeString},
	LabelHTTPUrl:        {columnPathSpanHTTPURL, traceql.AttributeScopeSpan, traceql.TypeString},
}

// Fetch spansets from the block for the given TraceQL FetchSpansRequest. The request is checked for
// internal consistencies:  operand count matches the operation, all operands in each condition are identical
// types, and the operand type is compatible with the operation.
func (b *backendBlock) Fetch(ctx context.Context, req traceql.FetchSpansRequest, opts common.SearchOptions) (traceql.FetchSpansResponse, error) {
	err := checkConditions(req.Conditions)
	if err != nil {
		return traceql.FetchSpansResponse{}, fmt.Errorf("conditions invalid: %w", err)
	}

	coalesceConditions(&req)

	pf, rr, err := b.openForSearch(ctx, opts)
	if err != nil {
		return traceql.FetchSpansResponse{}, err
	}

	rgs := rowGroupsFromFile(pf, opts)

	iter, err := fetch(ctx, req, pf, rgs, b.meta.DedicatedColumns)
	if err != nil {
		return traceql.FetchSpansResponse{}, fmt.Errorf("creating fetch iter: %w", err)
	}

	return traceql.FetchSpansResponse{
		Results: iter,
		Bytes:   func() uint64 { return rr.BytesRead() },
	}, nil
}

func checkConditions(conditions []traceql.Condition) error {
	for _, cond := range conditions {
		opCount := len(cond.Operands)

		switch cond.Op {

		case traceql.OpNone:
			if opCount != 0 {
				return fmt.Errorf("operanion none must have 0 arguments. condition: %+v", cond)
			}

		case traceql.OpEqual, traceql.OpNotEqual,
			traceql.OpGreater, traceql.OpGreaterEqual,
			traceql.OpLess, traceql.OpLessEqual,
			traceql.OpRegex, traceql.OpNotRegex:
			if opCount != 1 {
				return fmt.Errorf("operation %v must have exactly 1 argument. condition: %+v", cond.Op, cond)
			}

		default:
			return fmt.Errorf("unknown operation. condition: %+v", cond)
		}

		// Verify all operands are of the same type
		if opCount == 0 {
			continue
		}

		for i := 1; i < opCount; i++ {
			if reflect.TypeOf(cond.Operands[0]) != reflect.TypeOf(cond.Operands[i]) {
				return fmt.Errorf("operands must be of the same type. condition: %+v", cond)
			}
		}
	}

	return nil
}

func operandType(operands traceql.Operands) traceql.StaticType {
	if len(operands) > 0 {
		return operands[0].Type
	}
	return traceql.TypeNil
}

// spansetIterator turns the parquet iterator into the final
// traceql iterator.  Every row it receives is one spanset.
var _ parquetquery.Iterator = (*bridgeIterator)(nil)

// bridgeIterator creates a bridge between one iterator pass and the next
type bridgeIterator struct {
	iter parquetquery.Iterator
	cb   traceql.SecondPassFn

	nextSpans    []*span
	nextSpansIdx int
	at           *parquetquery.IteratorResult
}

func newBridgeIterator(iter parquetquery.Iterator, cb traceql.SecondPassFn) *bridgeIterator {
	return &bridgeIterator{
		iter: iter,
		cb:   cb,
		at:   parquetquery.DefaultPool.Get(),
	}
}

func (i *bridgeIterator) String() string {
	return fmt.Sprintf("bridgeIterator: \n\t%s", util.TabOut(i.iter))
}

// reset the buffer and reuse existing space.
func (i *bridgeIterator) reset() {
	i.nextSpansIdx = 0
	i.nextSpans = i.nextSpans[:0]
}

// pop next span from beginning of the buffer.
func (i *bridgeIterator) pop() (*parquetquery.IteratorResult, bool) {
	if i.nextSpansIdx >= len(i.nextSpans) {
		return nil, false
	}

	ret := i.nextSpans[i.nextSpansIdx]
	i.nextSpansIdx++
	return i.spanToIteratorResult(ret), true
}

func (i *bridgeIterator) Next() (*parquetquery.IteratorResult, error) {
	// drain current buffer
	if ret, ok := i.pop(); ok {
		return ret, nil
	}

	// get next spanset
	i.reset()

	for {
		res, err := i.iter.Next()
		if err != nil {
			return nil, err
		}
		if res == nil {
			return nil, nil
		}

		// The spanset is in the OtherEntries
		iface := res.OtherValueFromKey(otherEntrySpansetKey)
		if iface == nil {
			return nil, fmt.Errorf("engine assumption broken: spanset not found in other entries in bridge")
		}
		spanset, ok := iface.(*traceql.Spanset)
		if !ok {
			return nil, fmt.Errorf("engine assumption broken: spanset is not of type *traceql.Spanset in bridge")
		}

		filteredSpansets, err := i.cb(spanset)
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		// if the filter removed all spansets then let's release all back to the pool
		// no reason to try anything more nuanced than this. it will handle nearly all cases
		if len(filteredSpansets) == 0 {
			for _, s := range spanset.Spans {
				putSpan(s.(*span))
			}
			putSpanset(spanset)
		}

		// flatten spans into i.currentSpans
		for _, ss := range filteredSpansets {
			for idx, s := range ss.Spans {
				span := s.(*span)

				// mark whether this is the last span in the spanset
				span.cbSpansetFinal = idx == len(ss.Spans)-1
				span.cbSpanset = ss
				i.nextSpans = append(i.nextSpans, span)
			}
		}

		sort.Slice(i.nextSpans, func(j, k int) bool {
			return parquetquery.CompareRowNumbers(DefinitionLevelResourceSpansILSSpan, i.nextSpans[j].rowNum, i.nextSpans[k].rowNum) == -1
		})

		// found something!
		if ret, ok := i.pop(); ok {
			return ret, nil
		}
	}
}

func (i *bridgeIterator) spanToIteratorResult(s *span) *parquetquery.IteratorResult {
	res := i.at
	res.Reset()
	res.RowNumber = s.rowNum
	res.AppendOtherValue(otherEntrySpanKey, s)

	return res
}

func (i *bridgeIterator) SeekTo(to parquetquery.RowNumber, definitionLevel int) (*parquetquery.IteratorResult, error) {
	var at *parquetquery.IteratorResult

	for at, _ = i.Next(); i != nil && at != nil && parquetquery.CompareRowNumbers(definitionLevel, at.RowNumber, to) < 0; {
		at, _ = i.Next()
	}

	return at, nil
}

func (i *bridgeIterator) Close() {
	i.iter.Close()
	parquetquery.DefaultPool.Release(i.at)
}

// confirm rebatchIterator implements parquetquery.Iterator
var _ parquetquery.Iterator = (*rebatchIterator)(nil)

// rebatchIterator either passes spansets through directly OR rebatches them based on metadata
// in OtherEntries
type rebatchIterator struct {
	iter      parquetquery.Iterator
	at        *parquetquery.IteratorResult
	nextSpans []*span
}

func newRebatchIterator(iter parquetquery.Iterator) *rebatchIterator {
	return &rebatchIterator{
		iter: iter,
		at:   parquetquery.DefaultPool.Get(),
	}
}

func (i *rebatchIterator) String() string {
	return fmt.Sprintf("rebatchIterator: \n\t%s", util.TabOut(i.iter))
}

// Next has to handle two different style results. First is an initial set of spans
// that does not have a callback spanset. These can be passed directly through.
// Second is a set of spans that have spansets imposed by the callback (i.e. for grouping)
// these must be regrouped into the callback spansets
func (i *rebatchIterator) Next() (*parquetquery.IteratorResult, error) {
	for {
		// see if we have a queue
		res := i.resultFromNextSpans()
		if res != nil {
			return res, nil
		}

		// check the iterator for anything
		res, err := i.iter.Next()
		if err != nil {
			return nil, err
		}
		if res == nil {
			return nil, nil
		}

		// get the spanset and see if we should pass it through or buffer for rebatching
		iface := res.OtherValueFromKey(otherEntrySpansetKey)
		if iface == nil {
			return nil, fmt.Errorf("engine assumption broken: spanset not found in other entries in rebatch")
		}
		ss, ok := iface.(*traceql.Spanset)
		if !ok {
			return nil, fmt.Errorf("engine assumption broken: spanset is not of type *traceql.Spanset in rebatch")
		}

		// if this has no call back spanset just pass it on
		if len(ss.Spans) > 0 && ss.Spans[0].(*span).cbSpanset == nil {
			return res, nil
		}

		// dump all spans into our buffer
		for _, s := range ss.Spans {
			sp := s.(*span)
			if !sp.cbSpansetFinal {
				continue
			}

			// copy trace level data from the current iteration spanset into the rebatch spanset. only do this if
			// we don't have current data
			if sp.cbSpanset.DurationNanos == 0 {
				sp.cbSpanset.DurationNanos = ss.DurationNanos
			}
			if len(sp.cbSpanset.TraceID) == 0 {
				sp.cbSpanset.TraceID = ss.TraceID
			}
			if len(sp.cbSpanset.RootSpanName) == 0 {
				sp.cbSpanset.RootSpanName = ss.RootSpanName
			}
			if len(sp.cbSpanset.RootServiceName) == 0 {
				sp.cbSpanset.RootServiceName = ss.RootServiceName
			}
			if sp.cbSpanset.StartTimeUnixNanos == 0 {
				sp.cbSpanset.StartTimeUnixNanos = ss.StartTimeUnixNanos
			}
			if len(sp.cbSpanset.ServiceStats) == 0 {
				// If this spanset was created by a GroupOperation, the ServiceStats map is nil and must be allocated first
				if sp.cbSpanset.ServiceStats == nil {
					sp.cbSpanset.ServiceStats = map[string]traceql.ServiceStats{}
				}

				for service, stat := range ss.ServiceStats {
					sp.cbSpanset.ServiceStats[service] = traceql.ServiceStats{
						SpanCount:  stat.SpanCount,
						ErrorCount: stat.ErrorCount,
					}
				}
			}

			i.nextSpans = append(i.nextSpans, sp)
		}

		putSpanset(ss) // Repool the spanset but not the spans which have been moved to nextSpans as needed.

		res = i.resultFromNextSpans()
		if res != nil {
			return res, nil
		}
		// if we don't find anything in that spanset, start over
	}
}

func (i *rebatchIterator) resultFromNextSpans() *parquetquery.IteratorResult {
	for len(i.nextSpans) > 0 {
		ret := i.nextSpans[0]
		i.nextSpans = i.nextSpans[1:]

		if ret.cbSpansetFinal && ret.cbSpanset != nil {
			res := i.at
			res.Reset()
			res.AppendOtherValue(otherEntrySpansetKey, ret.cbSpanset)
			return res
		}
	}

	return nil
}

func (i *rebatchIterator) SeekTo(to parquetquery.RowNumber, definitionLevel int) (*parquetquery.IteratorResult, error) {
	return i.iter.SeekTo(to, definitionLevel)
}

func (i *rebatchIterator) Close() {
	i.iter.Close()
	parquetquery.DefaultPool.Release(i.at)
}

// spansetIterator turns the parquet iterator into the final
// traceql iterator.  Every row it receives is one spanset.
type spansetIterator struct {
	iter parquetquery.Iterator
}

var _ traceql.SpansetIterator = (*spansetIterator)(nil)

func newSpansetIterator(iter parquetquery.Iterator) *spansetIterator {
	return &spansetIterator{
		iter: iter,
	}
}

func (i *spansetIterator) Next(context.Context) (*traceql.Spanset, error) {
	res, err := i.iter.Next()
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}

	// The spanset is in the OtherEntries
	iface := res.OtherValueFromKey(otherEntrySpansetKey)
	if iface == nil {
		return nil, fmt.Errorf("engine assumption broken: spanset not found in other entries in spansetIterator")
	}
	ss, ok := iface.(*traceql.Spanset)
	if !ok {
		return nil, fmt.Errorf("engine assumption broken: spanset is not of type *traceql.Spanset in spansetIterator")
	}

	return ss, nil
}

func (i *spansetIterator) Close() {
	i.iter.Close()
}

// mergeSpansetIterator iterates through a slice of spansetIterators exhausting them
// in order
type mergeSpansetIterator struct {
	iters []traceql.SpansetIterator
}

var _ traceql.SpansetIterator = (*mergeSpansetIterator)(nil)

func (i *mergeSpansetIterator) Next(ctx context.Context) (*traceql.Spanset, error) {
	for len(i.iters) > 0 {
		spanset, err := i.iters[0].Next(ctx)
		if err != nil {
			return nil, err
		}
		if spanset == nil {
			// This iter is exhausted, pop it
			i.iters[0].Close()
			i.iters = i.iters[1:]
			continue
		}
		return spanset, nil
	}

	return nil, nil
}

func (i *mergeSpansetIterator) Close() {
	// Close any outstanding iters
	for _, iter := range i.iters {
		iter.Close()
	}
}

// fetch is the core logic for executing the given conditions against the parquet columns. The algorithm
// can be summarized as a hiearchy of iterators where we iterate related columns together and collect the results
// at each level into attributes, spans, and spansets.  Each condition (.foo=bar) is pushed down to the one or more
// matching columns using parquetquery.Predicates.  Results are collected The final return is an iterator where each result is 1 Spanset for each trace.
//
// Diagram:
//                                                                                          -------------
//  Link attribute iterator: key    ----------------------------------------------------->  | attribute |
//                          ...     ----------------------------------------------------->  | collector |
//  Link attribute iterator: valueN ----------------------------------------------------->  |           |
//                                                                                          -------------
//                                                                                                 |
//                                                                                                 |
//  Link column iterator 1 ---------------------------------------------------------------------   |
//  (ex: span ID, trace ID)                                                                    |   |
//                                                                                             |   |
//                                                                                             |   |
//                                                                                             V   V
//  Event attribute iterator: key    --------------------------------------------         -------------
//                            ...    -----------------------------------------  |         |  link     |
//  Event attribute iterator: valueN ---------------------------------------  | |         | collector |
//                                                                          | | |         -------------
//                                                                          V V V              |
//                                                                         -------------       |
//  Event column iterator 1 ---------------------------------------------  | attribute |       |
//     (ex: name, time since)                                           |  | collector |       |
//                                                                      |  -------------       |
//                                                                      |      |               |
//                                                                      V      V               |
//  Span attribute iterator: key    -------------------------         -------------            |
//                           ...    -----------------------  |        |  event    |            |
//  Span attribute iterator: valueN -------------------|  |  |        | collector |            |
//                                                     |  |  |        -------------            |
//                                                     V  V  V               |                 |
//                                                   -------------           |                 |
//                                                   | attribute |           | list            |
//                                                   | collector |           | of              |
//                                                   -------------           | events          |
//                                                            |              |                 |
//                                                            | List         |                 |
//                                                            | of span      |                 |
//                                                            | attributes   |                 |
//  Span column iterator 1    ---------------------------     |              |                 |
//                      ...   ------------------------  |     |              |                 |
//  Span column iterator N    ---------------------  |  |     |              |                 |
//    (ex: name, status)                          |  |  |     |              |                 |
//                                                V  V  V     V              V                 V
//                                               -------------------------------------------------
//                                               |                 span collector                |
//                                               -------------------------------------------------
//                                                                            |
//                                                                            | List of Spans
//                                                                            |
//                                                                            |
// Scope attribute iterator: key    -------------                             |
//                           ...    ----------   |                            |
// Scope attribute iterator: valueN -------   |  |                            |
// 									       |  |  |                            |
// 										   V  V  V                            |
// 									    -------------                         |
// 									    | attribute |--------------------     |
// 									    | collector |                   |     |
// 									    -------------                   |     |
//                                                                      |     |
// Scope column iterator 1  ------------------------------------------  |     |
// 					...  ---------------------------------------  |  |  |     |
// Scope column iterator N  ---------------------------------  |  |  |  |     |
//   (ex: name, version)                                       |  |  |  |     |
// 							                                   V  V  V  V     V
// 					                               		---------------------------
// 					                               	    |      scope collector     |
// 				                               			----------------------------
// 				                               				  |
// 											                  |
//  Resource attribute                                        |
//   iterators:                                               |
//     key     -----------------------------------------      |
//     ...     --------------------------------------  |      |
//     valueN  -----------------------------------  |  |      |
//                                               |  |  |      |
//                                               V  V  V      |
//                                            -------------   |
//                                            | attribute |   |
//                                            | collector |   |
//                                            -------------   |
//                                                      |     |
//                                                      |     |
//                                                      |     |
//                                                      |     |
// Resource column iterator 1  --------------------     |     |
//                      ...    -----------------  |     |     |
// Resource column iterator N  --------------  |  |     |     |
//    (ex: service.name)                    |  |  |     |     |
//                                          V  V  V     V     V
//                                         ----------------------
//                                         |   batch collector  |
//                                         ----------------------
//                                                            |
//                                                            | List of Spansets
// Trace column iterator 1  --------------------------        |
//                      ... -----------------------  |        |
// Trace column iterator N  --------------------  |  |        |
//    (ex: trace ID)                           |  |  |        |
//                                             V  V  V        V
//                                           -------------------
//                                           | trace collector |
//                                           -------------------
//                                                            |
//                                                            | Final Spanset
//                                                            |
//                                                            V

func fetch(ctx context.Context, req traceql.FetchSpansRequest, pf *parquet.File, rowGroups []parquet.RowGroup, dc backend.DedicatedColumns) (*spansetIterator, error) {
	iter, err := createAllIterator(ctx, nil, req.Conditions, req.AllConditions, req.StartTimeUnixNanos, req.EndTimeUnixNanos, rowGroups, pf, dc, false)
	if err != nil {
		return nil, fmt.Errorf("error creating iterator: %w", err)
	}

	if req.SecondPass != nil {
		iter = newBridgeIterator(newRebatchIterator(iter), req.SecondPass)

		iter, err = createAllIterator(ctx, iter, req.SecondPassConditions, false, 0, 0, rowGroups, pf, dc, req.SecondPassSelectAll)
		if err != nil {
			return nil, fmt.Errorf("error creating second pass iterator: %w", err)
		}
	}

	return newSpansetIterator(newRebatchIterator(iter)), nil
}

type categorizedConditions struct {
	span            []traceql.Condition
	instrumentation []traceql.Condition
	resource        []traceql.Condition
	trace           []traceql.Condition
	event           []traceql.Condition
	link            []traceql.Condition
}

// categorizeConditions categorizes conditions by scope
func categorizeConditions(conditions []traceql.Condition) (*categorizedConditions, bool, error) {
	var mingled bool
	var categorizedCond categorizedConditions

	for _, cond := range conditions {
		// If no-scoped intrinsic then assign default scope
		scope := cond.Attribute.Scope
		if cond.Attribute.Scope == traceql.AttributeScopeNone {
			if lookup, ok := intrinsicColumnLookups[cond.Attribute.Intrinsic]; ok {
				scope = lookup.scope
			}
		}

		switch scope {

		case traceql.AttributeScopeNone:
			mingled = true
			categorizedCond.span = append(categorizedCond.span, cond)
			categorizedCond.resource = append(categorizedCond.resource, cond)

		case traceql.AttributeScopeSpan, intrinsicScopeSpan:
			categorizedCond.span = append(categorizedCond.span, cond)

		case traceql.AttributeScopeResource:
			categorizedCond.resource = append(categorizedCond.resource, cond)

		case traceql.AttributeScopeEvent, intrinsicScopeEvent:
			categorizedCond.event = append(categorizedCond.event, cond)

		case traceql.AttributeScopeLink, intrinsicScopeLink:
			categorizedCond.link = append(categorizedCond.link, cond)

		case intrinsicScopeTrace:
			categorizedCond.trace = append(categorizedCond.trace, cond)

		case traceql.AttributeScopeInstrumentation, intrinsicScopeInstrumentation:
			categorizedCond.instrumentation = append(categorizedCond.instrumentation, cond)

		default:
			return nil, false, fmt.Errorf("unsupported traceql scope: %s", cond.Attribute)
		}
	}
	return &categorizedCond, mingled, nil
}

func createAllIterator(ctx context.Context, primaryIter parquetquery.Iterator, conditions []traceql.Condition, allConditions bool, start, end uint64, rgs []parquet.RowGroup, pf *parquet.File, dc backend.DedicatedColumns, selectAll bool,
) (parquetquery.Iterator, error) {
	// categorize conditions by scope
	catConditions, mingledConditions, err := categorizeConditions(conditions)
	if err != nil {
		return nil, err
	}

	makeIter := makeIterFunc(ctx, rgs, pf)

	// Global state
	// Span-filtering behavior changes depending on the resource-filtering in effect,
	// and vice-versa.  For example consider the query { span.a=1 }.  If no spans have a=1
	// then it generate the empty spanset.
	// However once we add a resource condition: { span.a=1 || resource.b=2 }, now the span
	// filtering must return all spans, even if no spans have a=1, because they might be
	// matched upstream to a resource.
	// TODO - After introducing AllConditions it seems like some of this logic overlaps.
	//        Determine if it can be generalized or simplified.

	// Don't return the final spanset upstream unless it matched at least 1 condition
	// anywhere, except in the case of the empty query: {}
	batchRequireAtLeastOneMatchOverall := len(conditions) > 0 && len(catConditions.trace) == 0

	// Optimization for queries like {resource.x... && span.y ...}
	// Requires no mingled scopes like .foo=x, which could be satisfied
	// one either resource or span.
	allConditions = allConditions && !mingledConditions

	innerIterators := make([]parquetquery.Iterator, 0, 3)
	if primaryIter != nil {
		innerIterators = append(innerIterators, primaryIter)
	}

	eventIter, err := createEventIterator(makeIter, catConditions.event, allConditions, selectAll)
	if err != nil {
		return nil, fmt.Errorf("creating event iterator: %w", err)
	}
	if eventIter != nil {
		innerIterators = append(innerIterators, eventIter)
	}

	linkIter, err := createLinkIterator(makeIter, catConditions.link, allConditions, selectAll)
	if err != nil {
		return nil, fmt.Errorf("creating link iterator: %w", err)
	}
	if linkIter != nil {
		innerIterators = append(innerIterators, linkIter)
	}

	spanIter, err := createSpanIterator(makeIter, innerIterators, catConditions.span, allConditions, dc, selectAll)
	if err != nil {
		return nil, fmt.Errorf("creating span iterator: %w", err)
	}

	instrumentationIter, err := createInstrumentationIterator(makeIter, spanIter, catConditions.instrumentation, allConditions, selectAll)
	if err != nil {
		return nil, fmt.Errorf("creating scope iterator: %w", err)
	}

	resourceIter, err := createResourceIterator(makeIter, instrumentationIter, catConditions.resource, batchRequireAtLeastOneMatchOverall, allConditions, dc, selectAll)
	if err != nil {
		return nil, fmt.Errorf("creating resource iterator: %w", err)
	}

	return createTraceIterator(makeIter, resourceIter, catConditions.trace, start, end, allConditions, selectAll)
}

func createEventIterator(makeIter makeIterFn, conditions []traceql.Condition, allConditions bool, selectAll bool) (parquetquery.Iterator, error) {
	if len(conditions) == 0 {
		return nil, nil
	}

	eventIters := make([]parquetquery.Iterator, 0, len(conditions))
	var genericConditions []traceql.Condition

	for _, cond := range conditions {
		switch cond.Attribute.Intrinsic {
		case traceql.IntrinsicEventName:
			pred, err := createStringPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, err
			}
			eventIters = append(eventIters, makeIter(columnPathEventName, pred, columnPathEventName))
			continue
		case traceql.IntrinsicEventTimeSinceStart:
			pred, err := createIntPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, err
			}
			eventIters = append(eventIters, makeIter(columnPathEventTimeSinceStart, pred, columnPathEventTimeSinceStart))
			continue
		}
		genericConditions = append(genericConditions, cond)
	}

	attrIter, err := createAttributeIterator(makeIter, genericConditions, DefinitionLevelResourceSpansILSSpanEventAttrs,
		columnPathEventAttrKey, columnPathEventAttrString, columnPathEventAttrInt, columnPathEventAttrDouble, columnPathEventAttrBool, allConditions, selectAll)
	if err != nil {
		return nil, fmt.Errorf("creating event attribute iterator: %w", err)
	}

	if attrIter != nil {
		eventIters = append(eventIters, attrIter)
	}

	var required []parquetquery.Iterator

	minCount := 0

	if allConditions {
		// The final number of expected attributes.
		distinct := map[string]struct{}{}
		for _, cond := range conditions {
			distinct[cond.Attribute.Name] = struct{}{}
		}
		minCount = len(distinct)
	}

	eventCol := &eventCollector{
		minAttributes: minCount,
	}

	// This is an optimization for when all of the span conditions must be met.
	// We simply move all iterators into the required list.
	if allConditions {
		required = append(required, eventIters...)
		eventIters = nil
	}

	if len(required) == 0 {
		required = []parquetquery.Iterator{makeIter(columnPathEventName, nil, "")}
	}

	if len(eventIters) == 0 && len(required) == 0 {
		return nil, nil
	}

	return parquetquery.NewLeftJoinIterator(DefinitionLevelResourceSpansILSSpanEvent, required, eventIters, eventCol, parquetquery.WithPool(pqEventPool))
}

func createLinkIterator(makeIter makeIterFn, conditions []traceql.Condition, allConditions, selectAll bool) (parquetquery.Iterator, error) {
	if len(conditions) == 0 {
		return nil, nil
	}

	linkIters := make([]parquetquery.Iterator, 0, len(conditions))
	var genericConditions []traceql.Condition

	for _, cond := range conditions {
		switch cond.Attribute.Intrinsic {
		case traceql.IntrinsicLinkTraceID:
			pred, err := createBytesPredicate(cond.Op, cond.Operands, false)
			if err != nil {
				return nil, err
			}
			linkIters = append(linkIters, makeIter(columnPathLinkTraceID, pred, columnPathLinkTraceID))
			continue

		case traceql.IntrinsicLinkSpanID:
			pred, err := createBytesPredicate(cond.Op, cond.Operands, true)
			if err != nil {
				return nil, err
			}
			linkIters = append(linkIters, makeIter(columnPathLinkSpanID, pred, columnPathLinkSpanID))
			continue
		}
		genericConditions = append(genericConditions, cond)
	}

	attrIter, err := createAttributeIterator(makeIter, genericConditions, DefinitionLevelResourceSpansILSSpanLinkAttrs,
		columnPathLinkAttrKey, columnPathLinkAttrString, columnPathLinkAttrInt, columnPathLinkAttrDouble, columnPathLinkAttrBool, allConditions, selectAll)
	if err != nil {
		return nil, fmt.Errorf("creating link attribute iterator: %w", err)
	}

	if attrIter != nil {
		linkIters = append(linkIters, attrIter)
	}

	var required []parquetquery.Iterator
	minCount := 0

	if allConditions {
		// The final number of expected attributes.
		distinct := map[string]struct{}{}
		for _, cond := range conditions {
			distinct[cond.Attribute.Name] = struct{}{}
		}
		minCount = len(distinct)
	}

	linkCol := &linkCollector{
		minAttributes: minCount,
	}

	if allConditions {
		required = append(required, linkIters...)
		linkIters = nil
	}

	if len(required) == 0 {
		required = []parquetquery.Iterator{makeIter(columnPathLinkSpanID, nil, "")}
	}

	if len(linkIters) == 0 && len(required) == 0 {
		return nil, nil
	}

	return parquetquery.NewLeftJoinIterator(DefinitionLevelResourceSpansILSSpanEvent, required, linkIters, linkCol, parquetquery.WithPool(pqLinkPool))
}

// createSpanIterator iterates through all span-level columns, groups them into rows representing
// one span each.  Spans are returned that match any of the given conditions.
func createSpanIterator(makeIter makeIterFn, innerIterators []parquetquery.Iterator, conditions []traceql.Condition, allConditions bool, dedicatedColumns backend.DedicatedColumns, selectAll bool) (parquetquery.Iterator, error) {
	var (
		columnSelectAs          = map[string]string{}
		columnPredicates        = map[string][]parquetquery.Predicate{}
		iters                   []parquetquery.Iterator
		genericConditions       []traceql.Condition
		columnMapping           = dedicatedColumnsToColumnMapping(dedicatedColumns, backend.DedicatedColumnScopeSpan)
		nestedSetLeftExplicit   = false
		nestedSetRightExplicit  = false
		nestedSetParentExplicit = false
	)

	// todo: improve these methods. if addPredicate gets a nil predicate shouldn't it just wipe out the existing predicates instead of appending?
	// nil predicate matches everything. what's the point of also evaluating a "real" predicate?
	addPredicate := func(columnPath string, p parquetquery.Predicate) {
		columnPredicates[columnPath] = append(columnPredicates[columnPath], p)
	}

	addNilPredicateIfNotAlready := func(path string) {
		preds := columnPredicates[path]
		foundOpNone := false

		// check to see if there is a nil predicate and only add if it doesn't exist
		for _, pred := range preds {
			if pred == nil {
				foundOpNone = true
				break
			}
		}

		if !foundOpNone {
			addPredicate(path, nil)
			columnSelectAs[path] = path
		}
	}

	for _, cond := range conditions {
		// Intrinsic?
		switch cond.Attribute.Intrinsic {
		case traceql.IntrinsicSpanID:
			pred, err := createBytesPredicate(cond.Op, cond.Operands, true)
			if err != nil {
				return nil, err
			}
			addPredicate(columnPathSpanID, pred)
			columnSelectAs[columnPathSpanID] = columnPathSpanID
			continue

		case traceql.IntrinsicParentID:
			pred, err := createBytesPredicate(cond.Op, cond.Operands, true)
			if err != nil {
				return nil, err
			}
			addPredicate(columnPathSpanParentSpanID, pred)
			columnSelectAs[columnPathSpanParentSpanID] = columnPathSpanParentSpanID
			continue

		case traceql.IntrinsicSpanStartTime:
			pred, err := createIntPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, err
			}
			addPredicate(columnPathSpanStartTime, pred)
			columnSelectAs[columnPathSpanStartTime] = columnPathSpanStartTime
			continue

		case traceql.IntrinsicName:
			pred, err := createStringPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, err
			}
			addPredicate(columnPathSpanName, pred)
			columnSelectAs[columnPathSpanName] = columnPathSpanName
			continue

		case traceql.IntrinsicKind:
			pred, err := createIntPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, err
			}
			addPredicate(columnPathSpanKind, pred)
			columnSelectAs[columnPathSpanKind] = columnPathSpanKind
			continue

		case traceql.IntrinsicDuration:
			pred, err := createDurationPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, err
			}
			addPredicate(columnPathSpanDuration, pred)
			columnSelectAs[columnPathSpanDuration] = columnPathSpanDuration
			continue

		case traceql.IntrinsicStatus:
			pred, err := createIntPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, err
			}
			addPredicate(columnPathSpanStatusCode, pred)
			columnSelectAs[columnPathSpanStatusCode] = columnPathSpanStatusCode
			continue
		case traceql.IntrinsicStatusMessage:
			pred, err := createStringPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, err
			}
			addPredicate(columnPathSpanStatusMessage, pred)
			columnSelectAs[columnPathSpanStatusMessage] = columnPathSpanStatusMessage
			continue

		case traceql.IntrinsicStructuralDescendant:
			addNilPredicateIfNotAlready(columnPathSpanNestedSetLeft)
			addNilPredicateIfNotAlready(columnPathSpanNestedSetRight)
			continue

		case traceql.IntrinsicStructuralChild:
			addNilPredicateIfNotAlready(columnPathSpanNestedSetLeft)
			addNilPredicateIfNotAlready(columnPathSpanParentID)
			continue

		case traceql.IntrinsicStructuralSibling:
			addNilPredicateIfNotAlready(columnPathSpanParentID)
			continue

		case traceql.IntrinsicNestedSetLeft:
			nestedSetLeftExplicit = true
			pred, err := createIntPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, err
			}
			addPredicate(columnPathSpanNestedSetLeft, pred)
			columnSelectAs[columnPathSpanNestedSetLeft] = columnPathSpanNestedSetLeft
			continue
		case traceql.IntrinsicNestedSetRight:
			nestedSetRightExplicit = true
			pred, err := createIntPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, err
			}
			addPredicate(columnPathSpanNestedSetRight, pred)
			columnSelectAs[columnPathSpanNestedSetRight] = columnPathSpanNestedSetRight
			continue
		case traceql.IntrinsicNestedSetParent:
			nestedSetParentExplicit = true
			pred, err := createIntPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, err
			}
			addPredicate(columnPathSpanParentID, pred)
			columnSelectAs[columnPathSpanParentID] = columnPathSpanParentID
			continue

		}

		// Well-known attribute?
		if entry, ok := wellKnownColumnLookups[cond.Attribute.Name]; ok && entry.level != traceql.AttributeScopeResource {
			if cond.Op == traceql.OpNone {
				addPredicate(entry.columnPath, nil) // No filtering
				columnSelectAs[entry.columnPath] = cond.Attribute.Name
				continue
			}

			// Compatible type?
			if entry.typ == operandType(cond.Operands) {
				pred, err := createPredicate(cond.Op, cond.Operands)
				if err != nil {
					return nil, fmt.Errorf("creating predicate: %w", err)
				}
				addPredicate(entry.columnPath, pred)
				columnSelectAs[entry.columnPath] = cond.Attribute.Name
				continue
			}
		}

		// Attributes stored in dedicated columns
		if c, ok := columnMapping.get(cond.Attribute.Name); ok {
			if cond.Op == traceql.OpNone {
				addPredicate(c.ColumnPath, nil) // No filtering
				columnSelectAs[c.ColumnPath] = cond.Attribute.Name
				continue
			}

			// Compatible type?
			typ, _ := c.Type.ToStaticType()
			if typ == operandType(cond.Operands) {
				pred, err := createPredicate(cond.Op, cond.Operands)
				if err != nil {
					return nil, fmt.Errorf("creating predicate: %w", err)
				}
				addPredicate(c.ColumnPath, pred)
				columnSelectAs[c.ColumnPath] = cond.Attribute.Name
				continue
			}
		}

		// Else: generic attribute lookup
		genericConditions = append(genericConditions, cond)
	}

	// SecondPass SelectAll
	if selectAll {
		for wellKnownAttr, entry := range wellKnownColumnLookups {
			if entry.level != traceql.AttributeScopeSpan {
				continue
			}

			addPredicate(entry.columnPath, nil)
			columnSelectAs[entry.columnPath] = wellKnownAttr
		}

		for intrins, entry := range intrinsicColumnLookups {
			if entry.scope != intrinsicScopeSpan {
				continue
			}
			// These intrinsics aren't included in select all because they
			// aren't useful for compare().
			switch intrins {
			case traceql.IntrinsicSpanID,
				traceql.IntrinsicParentID,
				traceql.IntrinsicSpanStartTime,
				traceql.IntrinsicStructuralDescendant,
				traceql.IntrinsicStructuralChild,
				traceql.IntrinsicStructuralSibling,
				traceql.IntrinsicNestedSetLeft,
				traceql.IntrinsicNestedSetRight,
				traceql.IntrinsicNestedSetParent:
				continue
			}
			addPredicate(entry.columnPath, nil)
			columnSelectAs[entry.columnPath] = entry.columnPath
		}

		for k, v := range columnMapping.mapping {
			addPredicate(v.ColumnPath, nil)
			columnSelectAs[v.ColumnPath] = k
		}
	}

	for columnPath, predicates := range columnPredicates {
		iters = append(iters, makeIter(columnPath, orIfNeeded(predicates), columnSelectAs[columnPath]))
	}

	attrIter, err := createAttributeIterator(makeIter, genericConditions, DefinitionLevelResourceSpansILSSpanAttrs,
		columnPathSpanAttrKey, columnPathSpanAttrString, columnPathSpanAttrInt, columnPathSpanAttrDouble, columnPathSpanAttrBool, allConditions, selectAll)
	if err != nil {
		return nil, fmt.Errorf("creating span attribute iterator: %w", err)
	}
	if attrIter != nil {
		iters = append(iters, attrIter)
	}

	var required []parquetquery.Iterator
	if len(innerIterators) != 0 {
		required = innerIterators
	}

	minCount := 0
	if allConditions {
		// The final number of expected attributes.
		distinct := map[string]struct{}{}
		for _, cond := range conditions {
			distinct[cond.Attribute.Name] = struct{}{}
		}
		minCount = len(distinct)
	}

	spanCol := &spanCollector{
		minAttributes:           minCount,
		nestedSetLeftExplicit:   nestedSetLeftExplicit,
		nestedSetRightExplicit:  nestedSetRightExplicit,
		nestedSetParentExplicit: nestedSetParentExplicit,
	}

	// This is an optimization for when all of the span conditions must be met.
	// We simply move all iterators into the required list.
	if allConditions {
		required = append(iters, required...)
		iters = nil
	}

	// if there are no direct conditions imposed on the span/span attributes level we are purposefully going to request the "Kind" column
	//  b/c it is extremely cheap to retrieve. retrieving matching spans in this case will allow aggregates such as "count" to be computed
	//  how do we know to pull duration for things like | avg(duration) > 1s? look at avg(span.http.status_code) it pushes a column request down here
	//  the entire engine is built around spans. we have to return at least one entry for every span to the layers above for things to work
	// TODO: note that if the query is { kind = client } the fetch layer will actually create two iterators over the kind column. this is evidence
	//  this spaniterator code could be tightened up
	// Also note that this breaks optimizations related to requireAtLeastOneMatch and requireAtLeastOneMatchOverall b/c it will add a kind attribute
	//  to the span attributes map in spanCollector
	if len(required) == 0 {
		required = []parquetquery.Iterator{makeIter(columnPathSpanStatusCode, nil, "")}
	}

	// Left join here means the span id/start/end iterators + 1 are required,
	// and all other conditions are optional. Whatever matches is returned.
	return parquetquery.NewLeftJoinIterator(DefinitionLevelResourceSpansILSSpan, required, iters, spanCol, parquetquery.WithPool(pqSpanPool))
}

func createInstrumentationIterator(makeIter makeIterFn, spanIterator parquetquery.Iterator, conditions []traceql.Condition, allConditions, selectAll bool) (parquetquery.Iterator, error) {
	var (
		iters             = []parquetquery.Iterator{}
		genericConditions []traceql.Condition
	)

	for _, cond := range conditions {

		// Intrinsics ?
		switch cond.Attribute.Intrinsic {
		case traceql.IntrinsicInstrumentationName:
			pred, err := createStringPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, err
			}
			iters = append(iters, makeIter(columnPathInstrumentationName, pred, columnPathInstrumentationName))
			continue

		case traceql.IntrinsicInstrumentationVersion:
			pred, err := createStringPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, err
			}
			iters = append(iters, makeIter(columnPathInstrumentationVersion, pred, columnPathInstrumentationVersion))
			continue
		}

		// Else: generic attribute lookup
		genericConditions = append(genericConditions, cond)
	}

	if selectAll {
		for _, entry := range intrinsicColumnLookups {
			if entry.scope != intrinsicScopeInstrumentation {
				continue
			}
			iters = append(iters, makeIter(entry.columnPath, nil, entry.columnPath))
		}
	}

	attrIter, err := createAttributeIterator(makeIter, genericConditions, DefinitionLevelInstrumentationScopeAttrs,
		columnPathInstrumentationAttrKey, columnPathInstrumentationAttrString, columnPathInstrumentationAttrInt, columnPathInstrumentationAttrDouble, columnPathInstrumentationAttrBool, allConditions, selectAll)
	if err != nil {
		return nil, fmt.Errorf("creating instrumentation attribute iterator: %w", err)
	}
	if attrIter != nil {
		iters = append(iters, attrIter)
	}

	minCount := 0
	if allConditions {
		// The final number of expected attributes
		distinct := map[string]struct{}{}
		for _, cond := range conditions {
			distinct[cond.Attribute.Name] = struct{}{}
		}
		minCount = len(distinct)
	}
	instrumentationCol := newInstrumentationCollector(minCount)

	var required []parquetquery.Iterator

	// This is an optimization for when all of the resource conditions must be met.
	// We simply move all iterators into the required list.
	if allConditions {
		required = append(required, iters...)
		iters = nil
	}

	// Put span iterator last so it is only read when
	// the scope conditions are met.
	required = append(required, spanIterator)

	// Left join here means the span iterator + 1 are required,
	// and all other resource conditions are optional. Whatever matches
	// is returned.
	return parquetquery.NewLeftJoinIterator(DefinitionLevelInstrumentationScope, required, iters, instrumentationCol, parquetquery.WithPool(pqInstrumentationPool))
}

// createResourceIterator iterates through all resourcespans-level (batch-level) columns, groups them into rows representing
// one batch each. It builds on top of the span iterator, and turns the groups of spans and resource-level values into
// spansets. Spansets are returned that match any of the given conditions.
func createResourceIterator(makeIter makeIterFn, instrumentationIterator parquetquery.Iterator, conditions []traceql.Condition, requireAtLeastOneMatchOverall, allConditions bool, dedicatedColumns backend.DedicatedColumns, selectAll bool) (parquetquery.Iterator, error) {
	var (
		columnSelectAs    = map[string]string{}
		columnPredicates  = map[string][]parquetquery.Predicate{}
		iters             = []parquetquery.Iterator{}
		genericConditions []traceql.Condition
		columnMapping     = dedicatedColumnsToColumnMapping(dedicatedColumns, backend.DedicatedColumnScopeResource)
	)

	addPredicate := func(columnPath string, p parquetquery.Predicate) {
		columnPredicates[columnPath] = append(columnPredicates[columnPath], p)
	}

	for _, cond := range conditions {

		// Well-known selector?
		if entry, ok := wellKnownColumnLookups[cond.Attribute.Name]; ok && entry.level != traceql.AttributeScopeSpan {
			if cond.Op == traceql.OpNone {
				addPredicate(entry.columnPath, nil) // No filtering
				columnSelectAs[entry.columnPath] = cond.Attribute.Name
				continue
			}

			// Compatible type?
			if entry.typ == operandType(cond.Operands) {
				pred, err := createPredicate(cond.Op, cond.Operands)
				if err != nil {
					return nil, fmt.Errorf("creating predicate: %w", err)
				}
				iters = append(iters, makeIter(entry.columnPath, pred, cond.Attribute.Name))
				continue
			}
		}

		// Attributes stored in dedicated columns
		if c, ok := columnMapping.get(cond.Attribute.Name); ok {
			if cond.Op == traceql.OpNone {
				addPredicate(c.ColumnPath, nil) // No filtering
				columnSelectAs[c.ColumnPath] = cond.Attribute.Name
				continue
			}

			// Compatible type?
			typ, _ := c.Type.ToStaticType()
			if typ == operandType(cond.Operands) {
				pred, err := createPredicate(cond.Op, cond.Operands)
				if err != nil {
					return nil, fmt.Errorf("creating predicate: %w", err)
				}
				addPredicate(c.ColumnPath, pred)
				columnSelectAs[c.ColumnPath] = cond.Attribute.Name
				continue
			}
		}

		// Else: generic attribute lookup
		genericConditions = append(genericConditions, cond)
	}

	// SecondPass SelectAll
	if selectAll {
		for wellKnownAttr, entry := range wellKnownColumnLookups {
			if entry.level != traceql.AttributeScopeResource {
				continue
			}

			addPredicate(entry.columnPath, nil)
			columnSelectAs[entry.columnPath] = wellKnownAttr
		}

		for k, v := range columnMapping.mapping {
			addPredicate(v.ColumnPath, nil)
			columnSelectAs[v.ColumnPath] = k
		}
	}

	for columnPath, predicates := range columnPredicates {
		iters = append(iters, makeIter(columnPath, orIfNeeded(predicates), columnSelectAs[columnPath]))
	}

	attrIter, err := createAttributeIterator(makeIter, genericConditions, DefinitionLevelResourceAttrs,
		columnPathResourceAttrKey, columnPathResourceAttrString, columnPathResourceAttrInt, columnPathResourceAttrDouble, columnPathResourceAttrBool, allConditions, selectAll)
	if err != nil {
		return nil, fmt.Errorf("creating span attribute iterator: %w", err)
	}
	if attrIter != nil {
		iters = append(iters, attrIter)
	}

	minCount := 0
	if allConditions {
		// The final number of expected attributes
		distinct := map[string]struct{}{}
		for _, cond := range conditions {
			distinct[cond.Attribute.Name] = struct{}{}
		}
		minCount = len(distinct)
	}
	batchCol := newBatchCollector(requireAtLeastOneMatchOverall, minCount)

	var required []parquetquery.Iterator

	// This is an optimization for when all of the resource conditions must be met.
	// We simply move all iterators into the required list.
	if allConditions {
		required = append(required, iters...)
		iters = nil
	}

	// Put span iterator last so it is only read when
	// the resource conditions are met.
	required = append(required, instrumentationIterator)

	// Left join here means the span iterator + 1 are required,
	// and all other resource conditions are optional. Whatever matches
	// is returned.
	return parquetquery.NewLeftJoinIterator(DefinitionLevelResourceSpans, required, iters, batchCol, parquetquery.WithPool(pqSpansetPool))
}

func createServiceStatsIterator(makeIter makeIterFn) parquetquery.Iterator {
	serviceStatsIters := []parquetquery.Iterator{
		makeIter(columnPathServiceStatsServiceName, nil, columnPathServiceStatsServiceName),
		makeIter(columnPathServiceStatsSpanCount, nil, columnPathServiceStatsSpanCount),
		makeIter(columnPathServiceStatsErrorCount, nil, columnPathServiceStatsErrorCount),
	}
	return parquetquery.NewJoinIterator(DefinitionLevelServiceStats, serviceStatsIters, &serviceStatsCollector{})
}

func createTraceIterator(makeIter makeIterFn, resourceIter parquetquery.Iterator, conds []traceql.Condition, start, end uint64, allConditions bool, selectAll bool) (parquetquery.Iterator, error) {
	iters := make([]parquetquery.Iterator, 0, 3)
	metaIters := make([]parquetquery.Iterator, 0)

	if selectAll {
		for intrins, entry := range intrinsicColumnLookups {
			if entry.scope != intrinsicScopeTrace {
				continue
			}
			// These intrinsics aren't included in select all because they are not
			// useful for filtering or grouping.
			switch intrins {
			case traceql.IntrinsicTraceStartTime,
				traceql.IntrinsicServiceStats:
				continue
			}
			iters = append(iters, makeIter(entry.columnPath, nil, entry.columnPath))
		}
	} else {
		// add conditional iterators first. this way if someone searches for { traceDuration > 1s && span.foo = "bar"} the query will
		// be sped up by searching for traceDuration first. note that we can only set the predicates if all conditions is true.
		// otherwise we just pass the info up to the engine to make a choice
		for _, cond := range conds {
			switch cond.Attribute.Intrinsic {
			case traceql.IntrinsicTraceID:
				if cond.Op == traceql.OpNone && cond.CallBack != nil {
					metaIters = append(metaIters, makeIter(columnPathTraceID, parquetquery.NewCallbackPredicate(cond.CallBack), columnPathTraceID))
				} else {
					pred, err := createBytesPredicate(cond.Op, cond.Operands, false)
					if err != nil {
						return nil, err
					}
					iters = append(iters, makeIter(columnPathTraceID, pred, columnPathTraceID))
				}
			case traceql.IntrinsicTraceDuration:
				pred, err := createDurationPredicate(cond.Op, cond.Operands)
				if err != nil {
					return nil, err
				}
				iters = append(iters, makeIter(columnPathDurationNanos, pred, columnPathDurationNanos))
			case traceql.IntrinsicTraceStartTime:
				if start == 0 && end == 0 {
					iters = append(iters, makeIter(columnPathStartTimeUnixNano, nil, columnPathStartTimeUnixNano))
				}
			case traceql.IntrinsicTraceRootSpan:
				pred, err := createStringPredicate(cond.Op, cond.Operands)
				if err != nil {
					return nil, err
				}
				iters = append(iters, makeIter(columnPathRootSpanName, pred, columnPathRootSpanName))
			case traceql.IntrinsicTraceRootService:
				pred, err := createStringPredicate(cond.Op, cond.Operands)
				if err != nil {
					return nil, err
				}
				iters = append(iters, makeIter(columnPathRootServiceName, pred, columnPathRootServiceName))
			case traceql.IntrinsicServiceStats:
				iters = append(iters, createServiceStatsIterator(makeIter))
			}
		}
	}

	var required []parquetquery.Iterator

	// This is an optimization for when all the conditions must be met.
	// We simply move all iterators into the required list.
	if allConditions {
		required = append(required, iters...)
		iters = nil
	}

	// order is interesting here. would it be more efficient to grab the span/resource conditions first
	// or the time range filtering first?
	required = append(required, resourceIter)

	// evaluate time range
	// Time range filtering?
	if start > 0 && end > 0 {
		// Here's how we detect the span overlaps the time window:
		// Span start <= req.End
		// Span end >= req.Start
		var startFilter, endFilter parquetquery.Predicate
		startFilter = parquetquery.NewIntBetweenPredicate(0, int64(end))
		endFilter = parquetquery.NewIntBetweenPredicate(int64(start), math.MaxInt64)

		required = append(required, makeIter(columnPathStartTimeUnixNano, startFilter, columnPathStartTimeUnixNano))
		required = append(required, makeIter(columnPathEndTimeUnixNano, endFilter, columnPathEndTimeUnixNano))
	}

	// Append meta iterators if there are any (exemplars)
	iters = append(iters, metaIters...)

	// Final trace iterator
	// TraceCollector adds trace-level data to the spansets
	return parquetquery.NewLeftJoinIterator(DefinitionLevelTrace, required, iters, newTraceCollector(), parquetquery.WithPool(pqTracePool))
}

func createPredicate(op traceql.Operator, operands traceql.Operands) (parquetquery.Predicate, error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	switch operands[0].Type {
	case traceql.TypeString:
		return createStringPredicate(op, operands)
	case traceql.TypeInt:
		return createIntPredicate(op, operands)
	case traceql.TypeFloat:
		return createFloatPredicate(op, operands)
	case traceql.TypeBoolean:
		return createBoolPredicate(op, operands)
	default:
		return nil, fmt.Errorf("cannot create predicate for operand: %v", operands[0])
	}
}

func createStringPredicate(op traceql.Operator, operands traceql.Operands) (parquetquery.Predicate, error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	s := operands[0].EncodeToString(false)
	if operands[0].Type != traceql.TypeString {
		return nil, fmt.Errorf("operand is not string: %s", s)
	}

	switch op {
	case traceql.OpEqual:
		return parquetquery.NewStringEqualPredicate([]byte(s)), nil
	case traceql.OpNotEqual:
		return parquetquery.NewStringNotEqualPredicate([]byte(s)), nil
	case traceql.OpRegex:
		return parquetquery.NewRegexInPredicate([]string{s})
	case traceql.OpNotRegex:
		return parquetquery.NewRegexNotInPredicate([]string{s})
	case traceql.OpGreater:
		return parquetquery.NewStringGreaterPredicate([]byte(s)), nil
	case traceql.OpGreaterEqual:
		return parquetquery.NewStringGreaterEqualPredicate([]byte(s)), nil
	case traceql.OpLess:
		return parquetquery.NewStringLessPredicate([]byte(s)), nil
	case traceql.OpLessEqual:
		return parquetquery.NewStringLessEqualPredicate([]byte(s)), nil
	default:
		return nil, fmt.Errorf("operator not supported for strings: %+v", op)
	}
}

func createBytesPredicate(op traceql.Operator, operands traceql.Operands, isSpan bool) (parquetquery.Predicate, error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	s := operands[0].EncodeToString(false)
	if operands[0].Type != traceql.TypeString {
		return nil, fmt.Errorf("operand is not string: %s", s)
	}

	var id []byte
	var err error
	if isSpan {
		id, err = util.HexStringToSpanID(s)
	} else {
		id, err = util.HexStringToTraceID(s)
	}

	if err != nil {
		return nil, nil
	}

	switch op {
	case traceql.OpEqual:
		return parquetquery.NewByteEqualPredicate(id), nil
	case traceql.OpNotEqual:
		return parquetquery.NewByteNotEqualPredicate(id), nil
	default:
		return nil, fmt.Errorf("operator not supported for IDs: %+v", op)
	}
}

func createDurationPredicate(op traceql.Operator, operands traceql.Operands) (parquetquery.Predicate, error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	if operands[0].Type == traceql.TypeFloat {
		// The column is already indexed as int, so we need to convert the float to int
		return createIntPredicateFromFloat(op, operands)
	}

	return createIntPredicate(op, operands)
}

// createIntPredicateFromFloat adapts a float-based query operand to an int column.
// If the float is exactly representable as an int64 (e.g. 42.0), we compare the
// column to that integer. Otherwise, if the float is non-integer or out of the
// int64 range, we return a "trivial" outcome:
//
//   - "=" on a non-integer float returns nil, meaning "no filter"
//   - "!=" on a non-integer float always matches, implemented as a predicate
//     that returns true for every row.
//   - For "<", "<=", ">", ">=", we shift the boundary to the nearest integer.
//     For example, "x < 10.3" becomes "x <= 10" for the int column.
//
// Note: If returning nil, no column-level filtering is applied for this condition.
func createIntPredicateFromFloat(op traceql.Operator, operands traceql.Operands) (parquetquery.Predicate, error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	if operands[0].Type != traceql.TypeFloat {
		return nil, fmt.Errorf("operand is not float: %s", operands[0].EncodeToString(false))
	}
	f := operands[0].Float()

	if math.IsNaN(f) {
		return nil, nil
	}

	// Check if it's in [MinInt64, MaxInt64) range, and if so, see if it's an integer.
	if float64(math.MinInt64) <= f && f < float64(math.MaxInt64) {
		if intPart, frac := math.Modf(f); frac == 0 {
			intOperands := traceql.Operands{traceql.NewStaticInt(int(intPart))}
			return createIntPredicate(op, intOperands)
		}
	}

	switch op {
	case traceql.OpEqual:
		return nil, nil
	case traceql.OpNotEqual:
		return parquetquery.NewCallbackPredicate(func() bool { return true }), nil
	case traceql.OpGreater, traceql.OpGreaterEqual:
		switch {
		case f < float64(math.MinInt64):
			return parquetquery.NewCallbackPredicate(func() bool { return true }), nil
		case float64(math.MaxInt64) <= f:
			return nil, nil
		case 0 < f:
			// "x > 10.3" -> "x >= 11"
			return parquetquery.NewIntGreaterEqualPredicate(int64(f) + 1), nil
		default:
			// "x > -2.7" -> "x >= -2"
			return parquetquery.NewIntGreaterEqualPredicate(int64(f)), nil
		}
	case traceql.OpLess, traceql.OpLessEqual:
		switch {
		case f < float64(math.MinInt64):
			return nil, nil
		case float64(math.MaxInt64) <= f:
			return parquetquery.NewCallbackPredicate(func() bool { return true }), nil
		case f < 0:
			// "x < -2.7" -> "x <= -3"
			return parquetquery.NewIntLessEqualPredicate(int64(f) - 1), nil
		default:
			// "x < 10.3" -> "x <= 10"
			return parquetquery.NewIntLessEqualPredicate(int64(f)), nil
		}
	}

	return nil, fmt.Errorf("operator not supported for integers: %v", op)
}

func createIntPredicate(op traceql.Operator, operands traceql.Operands) (parquetquery.Predicate, error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	var i int64
	switch operands[0].Type {
	case traceql.TypeInt:
		n, _ := operands[0].Int()
		i = int64(n)
	case traceql.TypeDuration:
		d, _ := operands[0].Duration()
		i = d.Nanoseconds()
	case traceql.TypeStatus:
		st, _ := operands[0].Status()
		i = int64(StatusCodeMapping[st.String()])
	case traceql.TypeKind:
		k, _ := operands[0].Kind()
		i = int64(KindMapping[k.String()])
	default:
		return nil, fmt.Errorf("operand is not int, duration, status or kind: %s", operands[0].EncodeToString(false))
	}

	switch op {
	case traceql.OpEqual:
		return parquetquery.NewIntEqualPredicate(i), nil
	case traceql.OpNotEqual:
		return parquetquery.NewIntNotEqualPredicate(i), nil
	case traceql.OpGreater:
		return parquetquery.NewIntGreaterPredicate(i), nil
	case traceql.OpGreaterEqual:
		return parquetquery.NewIntGreaterEqualPredicate(i), nil
	case traceql.OpLess:
		return parquetquery.NewIntLessPredicate(i), nil
	case traceql.OpLessEqual:
		return parquetquery.NewIntLessEqualPredicate(i), nil
	default:
		return nil, fmt.Errorf("operator not supported for integers: %+v", op)
	}
}

func createFloatPredicate(op traceql.Operator, operands traceql.Operands) (parquetquery.Predicate, error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	// Ensure operand is float
	if operands[0].Type != traceql.TypeFloat {
		return nil, fmt.Errorf("operand is not float: %s", operands[0].EncodeToString(false))
	}

	f := operands[0].Float()

	switch op {
	case traceql.OpEqual:
		return parquetquery.NewFloatEqualPredicate(f), nil
	case traceql.OpNotEqual:
		return parquetquery.NewFloatNotEqualPredicate(f), nil
	case traceql.OpGreater:
		return parquetquery.NewFloatGreaterPredicate(f), nil
	case traceql.OpGreaterEqual:
		return parquetquery.NewFloatGreaterEqualPredicate(f), nil
	case traceql.OpLess:
		return parquetquery.NewFloatLessPredicate(f), nil
	case traceql.OpLessEqual:
		return parquetquery.NewFloatLessEqualPredicate(f), nil
	default:
		return nil, fmt.Errorf("operator not supported for floats: %+v", op)
	}
}

func createBoolPredicate(op traceql.Operator, operands traceql.Operands) (parquetquery.Predicate, error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	// Ensure operand is bool
	b, ok := operands[0].Bool()
	if !ok {
		return nil, fmt.Errorf("oparand is not bool: %+v", operands[0].EncodeToString(false))
	}

	switch op {
	case traceql.OpEqual:
		return parquetquery.NewBoolEqualPredicate(b), nil
	case traceql.OpNotEqual:
		return parquetquery.NewBoolNotEqualPredicate(b), nil
	default:
		return nil, fmt.Errorf("operator not supported for booleans: %+v", op)
	}
}

func createAttributeIterator(makeIter makeIterFn, conditions []traceql.Condition,
	definitionLevel int,
	keyPath, strPath, intPath, floatPath, boolPath string,
	allConditions bool, selectAll bool,
) (parquetquery.Iterator, error) {
	if selectAll {
		// Select all with no filtering
		// Levels such as resource/instrumentation/span may have no attributes. When that
		// occurs the columns are encoded as single null values, and the current attribute
		// collector reads them as Nils.  We could skip them in the attribute collector,
		// but this is more performant because it's at the lowest level.
		// Alternatively, JoinIterators don't pay attention to -1 (undefined) when checking
		// the definition level matches.  Fixing that would also work but would need wider testing first.
		skipNils := &parquetquery.SkipNilsPredicate{}
		return parquetquery.NewLeftJoinIterator(definitionLevel,
			[]parquetquery.Iterator{
				makeIter(keyPath, skipNils, "key"),
			},
			[]parquetquery.Iterator{
				makeIter(strPath, skipNils, "string"),
				makeIter(intPath, skipNils, "int"),
				makeIter(floatPath, skipNils, "float"),
				makeIter(boolPath, skipNils, "bool"),
			},
			&attributeCollector{},
			parquetquery.WithPool(pqAttrPool))
	}

	var (
		attrKeys        = []string{}
		attrStringPreds = []parquetquery.Predicate{}
		attrIntPreds    = []parquetquery.Predicate{}
		attrFltPreds    = []parquetquery.Predicate{}
		boolPreds       = []parquetquery.Predicate{}
	)
	for _, cond := range conditions {

		attrKeys = append(attrKeys, cond.Attribute.Name)

		if cond.Op == traceql.OpNone {
			// This means we have to scan all values, we don't know what type
			// to expect
			attrStringPreds = append(attrStringPreds, nil)
			attrIntPreds = append(attrIntPreds, nil)
			attrFltPreds = append(attrFltPreds, nil)
			boolPreds = append(boolPreds, nil)
			continue
		}

		switch cond.Operands[0].Type {

		case traceql.TypeString:
			pred, err := createStringPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, fmt.Errorf("creating attribute predicate: %w", err)
			}
			attrStringPreds = append(attrStringPreds, pred)

		case traceql.TypeInt:
			pred, err := createIntPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, fmt.Errorf("creating attribute predicate: %w", err)
			}
			attrIntPreds = append(attrIntPreds, pred)

		case traceql.TypeFloat:
			pred, err := createFloatPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, fmt.Errorf("creating attribute predicate: %w", err)
			}
			attrFltPreds = append(attrFltPreds, pred)

		case traceql.TypeBoolean:
			pred, err := createBoolPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, fmt.Errorf("creating attribute predicate: %w", err)
			}
			boolPreds = append(boolPreds, pred)
		}
	}

	var valueIters []parquetquery.Iterator
	if len(attrStringPreds) > 0 {
		valueIters = append(valueIters, makeIter(strPath, orIfNeeded(attrStringPreds), "string"))
	}
	if len(attrIntPreds) > 0 {
		valueIters = append(valueIters, makeIter(intPath, orIfNeeded(attrIntPreds), "int"))
	}
	if len(attrFltPreds) > 0 {
		valueIters = append(valueIters, makeIter(floatPath, orIfNeeded(attrFltPreds), "float"))
	}
	if len(boolPreds) > 0 {
		valueIters = append(valueIters, makeIter(boolPath, orIfNeeded(boolPreds), "bool"))
	}

	if len(valueIters) > 0 {
		// LeftJoin means only look at rows where the key is what we want.
		// Bring in any of the typed values as needed.

		// if all conditions must be true we can use a simple join iterator to test the values one column at a time.
		// len(valueIters) must be 1 to handle queries like `{ span.foo = "x" && span.bar > 1}`
		if allConditions && len(valueIters) == 1 {
			iters := append([]parquetquery.Iterator{makeIter(keyPath, parquetquery.NewStringInPredicate(attrKeys), "key")}, valueIters...)
			return parquetquery.NewJoinIterator(definitionLevel,
				iters,
				&attributeCollector{},
				parquetquery.WithPool(pqAttrPool)), nil
		}

		return parquetquery.NewLeftJoinIterator(definitionLevel,
			[]parquetquery.Iterator{makeIter(keyPath, parquetquery.NewStringInPredicate(attrKeys), "key")},
			valueIters,
			&attributeCollector{},
			parquetquery.WithPool(pqAttrPool))
	}

	return nil, nil
}

// This turns groups of span values into Span objects
type spanCollector struct {
	minAttributes int

	nestedSetLeftExplicit   bool
	nestedSetRightExplicit  bool
	nestedSetParentExplicit bool
}

var _ parquetquery.GroupPredicate = (*spanCollector)(nil)

func (c *spanCollector) String() string {
	return fmt.Sprintf("spanCollector(%d)", c.minAttributes)
}

func (c *spanCollector) KeepGroup(res *parquetquery.IteratorResult) bool {
	var sp *span
	// look for existing span first. this occurs on the second pass
	for _, e := range res.OtherEntries {
		if v, ok := e.Value.(*span); ok {
			sp = v
			break
		}
	}

	// if not found create a new one
	if sp == nil {
		sp = getSpan()
		sp.rowNum = res.RowNumber
	}

	for _, e := range res.OtherEntries {
		switch v := e.Value.(type) {
		case traceql.Static:
			sp.addSpanAttr(newSpanAttr(e.Key), v)
		case *event:
			sp.setEventAttrs(v.attrs)
			putEvent(v)
		case *link:
			sp.setLinkAttrs(v.attrs)
			putLink(v)
		}
	}

	var durationNanos uint64

	// Merge all individual columns into the span
	for _, kv := range res.Entries {
		switch kv.Key {
		case columnPathSpanID:
			sp.id = kv.Value.ByteArray()
			sp.addSpanAttr(traceql.IntrinsicSpanIDAttribute, traceql.NewStaticString(util.SpanIDToHexString(kv.Value.ByteArray())))
		case columnPathSpanParentSpanID:
			sp.addSpanAttr(traceql.IntrinsicParentIDAttribute, traceql.NewStaticString(util.SpanIDToHexString(kv.Value.ByteArray())))
		case columnPathSpanStartTime:
			sp.startTimeUnixNanos = kv.Value.Uint64()
		case columnPathSpanDuration:
			durationNanos = kv.Value.Uint64()
			sp.durationNanos = durationNanos
			sp.addSpanAttr(traceql.IntrinsicDurationAttribute, traceql.NewStaticDuration(time.Duration(durationNanos)))
		case columnPathSpanName:
			sp.addSpanAttr(traceql.IntrinsicNameAttribute, traceql.NewStaticString(unsafeToString(kv.Value.Bytes())))
		case columnPathSpanStatusCode:
			sp.addSpanAttr(traceql.IntrinsicStatusAttribute, traceql.NewStaticStatus(otlpStatusToTraceqlStatus(kv.Value.Uint64())))
		case columnPathSpanStatusMessage:
			sp.addSpanAttr(traceql.IntrinsicStatusMessageAttribute, traceql.NewStaticString(unsafeToString(kv.Value.Bytes())))
		case columnPathSpanKind:
			sp.addSpanAttr(traceql.IntrinsicKindAttribute, traceql.NewStaticKind(otlpKindToTraceqlKind(kv.Value.Uint64())))
		case columnPathSpanParentID:
			sp.nestedSetParent = kv.Value.Int32()
			if c.nestedSetParentExplicit {
				sp.addSpanAttr(traceql.IntrinsicNestedSetParentAttribute, traceql.NewStaticInt(int(kv.Value.Int32())))
			}
		case columnPathSpanNestedSetLeft:
			sp.nestedSetLeft = kv.Value.Int32()
			if c.nestedSetLeftExplicit {
				sp.addSpanAttr(traceql.IntrinsicNestedSetLeftAttribute, traceql.NewStaticInt(int(kv.Value.Int32())))
			}
		case columnPathSpanNestedSetRight:
			sp.nestedSetRight = kv.Value.Int32()
			if c.nestedSetRightExplicit {
				sp.addSpanAttr(traceql.IntrinsicNestedSetRightAttribute, traceql.NewStaticInt(int(kv.Value.Int32())))
			}
		default:
			// TODO - This exists for span-level dedicated columns like http.status_code
			// Are nils possible here?
			switch kv.Value.Kind() {
			case parquet.Boolean:
				sp.addSpanAttr(newSpanAttr(kv.Key), traceql.NewStaticBool(kv.Value.Boolean()))
			case parquet.Int32, parquet.Int64:
				sp.addSpanAttr(newSpanAttr(kv.Key), traceql.NewStaticInt(int(kv.Value.Int64())))
			case parquet.Float:
				sp.addSpanAttr(newSpanAttr(kv.Key), traceql.NewStaticFloat(kv.Value.Double()))
			case parquet.ByteArray:
				sp.addSpanAttr(newSpanAttr(kv.Key), traceql.NewStaticString(unsafeToString(kv.Value.Bytes())))
			}
		}
	}

	if c.minAttributes > 0 {
		count := sp.attributesMatched()
		if count < c.minAttributes {
			putSpan(sp)
			return false
		}
	}

	res.Entries = res.Entries[:0]
	res.OtherEntries = res.OtherEntries[:0]
	res.AppendOtherValue(otherEntrySpanKey, sp)

	return true
}

// instrumentationCollector receives rows of matching scope-level
type instrumentationCollector struct {
	minAttributes        int
	instrumentationAttrs []attrVal
}

var _ parquetquery.GroupPredicate = (*instrumentationCollector)(nil)

func newInstrumentationCollector(minAttributes int) *instrumentationCollector {
	return &instrumentationCollector{
		minAttributes: minAttributes,
	}
}

func (c *instrumentationCollector) String() string {
	return fmt.Sprintf("instrumentationCollector(%d)", c.minAttributes)
}

func (c *instrumentationCollector) KeepGroup(res *parquetquery.IteratorResult) bool {
	// First pass over spans and attributes from the AttributeCollector
	spans := res.OtherEntries[:0]
	c.instrumentationAttrs = c.instrumentationAttrs[:0]

	for _, kv := range res.OtherEntries {
		switch v := kv.Value.(type) {
		case *span:
			spans = append(spans, kv)
		case traceql.Static:
			c.instrumentationAttrs = append(c.instrumentationAttrs, attrVal{newInstrumentationAttrs(kv.Key), v})
		}
	}
	res.OtherEntries = spans

	// Throw out batches without any candidate spans
	if len(res.OtherEntries) == 0 {
		return false
	}

	// scope intrinsics
	for _, kv := range res.Entries {
		switch kv.Key {
		case columnPathInstrumentationName:
			c.instrumentationAttrs = append(c.instrumentationAttrs, attrVal{
				a: traceql.IntrinsicInstrumentationNameAttribute,
				s: traceql.NewStaticString(unsafeToString(kv.Value.Bytes())),
			})
		case columnPathInstrumentationVersion:
			c.instrumentationAttrs = append(c.instrumentationAttrs, attrVal{
				a: traceql.IntrinsicInstrumentationVersionAttribute,
				s: traceql.NewStaticString(unsafeToString(kv.Value.Bytes())),
			})
		}
	}

	if c.minAttributes > 0 {
		if len(c.instrumentationAttrs) < c.minAttributes {
			return false
		}
	}

	// Second pass. Update spans with instrumentation attributes.
	if len(c.instrumentationAttrs) > 0 {
		for _, e := range res.OtherEntries {
			span, ok := e.Value.(*span)
			if !ok {
				continue
			}

			// Copy scope-level attributes to the span
			// If the span already has an entry for this attribute it
			// takes precedence (can be nil to indicate no match)
			span.setInstrumentationAttrs(c.instrumentationAttrs)
		}
	}

	res.Entries = res.Entries[:0]
	return true
}

// batchCollector receives rows of matching resource-level
// This turns groups of batch values and Spans into SpanSets
type batchCollector struct {
	requireAtLeastOneMatchOverall bool
	minAttributes                 int
	resAttrs                      []attrVal
}

var _ parquetquery.GroupPredicate = (*batchCollector)(nil)

func newBatchCollector(requireAtLeastOneMatchOverall bool, minAttributes int) *batchCollector {
	return &batchCollector{
		requireAtLeastOneMatchOverall: requireAtLeastOneMatchOverall,
		minAttributes:                 minAttributes,
	}
}

func (c *batchCollector) String() string {
	return fmt.Sprintf("batchCollector(%v, %d)", c.requireAtLeastOneMatchOverall, c.minAttributes)
}

// KeepGroup applies resource-level data and filtering to the spans yielded from
// the span-level iterators.  It updates the spans in-place in the OtherEntries slice.
// Creation of the spanset is delayed until the traceCollector.
func (c *batchCollector) KeepGroup(res *parquetquery.IteratorResult) bool {
	// First pass over spans and attributes from the AttributeCollector
	spans := res.OtherEntries[:0]
	c.resAttrs = c.resAttrs[:0]

	for _, kv := range res.OtherEntries {
		switch v := kv.Value.(type) {
		case *span:
			spans = append(spans, kv)
		case traceql.Static:
			c.resAttrs = append(c.resAttrs, attrVal{newResAttr(kv.Key), v})
		}
	}
	res.OtherEntries = spans

	// Throw out batches without any candidate spans
	if len(res.OtherEntries) == 0 {
		return false
	}

	// Gather Attributes from dedicated resource-level columns
	for _, e := range res.Entries {
		switch e.Value.Kind() {
		case parquet.Int64:
			c.resAttrs = append(c.resAttrs, attrVal{newResAttr(e.Key), traceql.NewStaticInt(int(e.Value.Int64()))})
		case parquet.ByteArray:
			c.resAttrs = append(c.resAttrs, attrVal{newResAttr(e.Key), traceql.NewStaticString(unsafeToString(e.Value.Bytes()))})
		}
	}

	if c.minAttributes > 0 {
		if len(c.resAttrs) < c.minAttributes {
			return false
		}
	}

	// Second pass. Update and further filter the spans
	if len(c.resAttrs) > 0 || c.requireAtLeastOneMatchOverall {
		mightFilter := c.requireAtLeastOneMatchOverall

		// If we might filter, then rebuild the slice of kept
		// spans, in place with the same underlying buffer.
		// If not filtering, then skip this work.
		var spans []struct {
			Key   string
			Value interface{}
		}
		if mightFilter {
			spans = res.OtherEntries[:0]
		}

		for _, e := range res.OtherEntries {
			span, ok := e.Value.(*span)
			if !ok {
				continue
			}

			// Copy resource-level attributes to the span
			// If the span already has an entry for this attribute it
			// takes precedence (can be nil to indicate no match)
			span.setResourceAttrs(c.resAttrs)

			if mightFilter {
				// Skip over span if it didn't meet minimum criteria
				if span.attributesMatched() == 0 {
					putSpan(span)
					continue
				}
				spans = append(spans, e)
			}
		}

		if mightFilter {
			res.OtherEntries = spans
		}
	}

	// Throw out batches without any remaining spans
	if len(res.OtherEntries) == 0 {
		return false
	}

	res.Entries = res.Entries[:0]
	return true
}

// traceCollector receives rows from the resource-level matches.
// It adds trace-level attributes into the spansets before
// they are returned
type traceCollector struct {
	// traceAttrs is a slice reused by KeepGroup to reduce allocations
	traceAttrs []attrVal
}

var _ parquetquery.GroupPredicate = (*traceCollector)(nil)

func newTraceCollector() *traceCollector {
	return &traceCollector{}
}

func (c *traceCollector) String() string {
	return "traceCollector()"
}

// KeepGroup is called once per trace and creates its final spanset
// containing all the matching spans.  These spans already contain all span-level and
// resource-level data.
func (c *traceCollector) KeepGroup(res *parquetquery.IteratorResult) bool {
	finalSpanset := getSpanset()
	c.traceAttrs = c.traceAttrs[:0]

	for _, e := range res.Entries {
		switch e.Key {
		case columnPathTraceID:
			finalSpanset.TraceID = e.Value.ByteArray()
			c.traceAttrs = append(c.traceAttrs, attrVal{traceql.IntrinsicTraceIDAttribute, traceql.NewStaticString(util.TraceIDToHexString(e.Value.ByteArray()))})
		case columnPathStartTimeUnixNano:
			finalSpanset.StartTimeUnixNanos = e.Value.Uint64()
		case columnPathDurationNanos:
			finalSpanset.DurationNanos = e.Value.Uint64()
			c.traceAttrs = append(c.traceAttrs, attrVal{traceql.IntrinsicTraceDurationAttribute, traceql.NewStaticDuration(time.Duration(finalSpanset.DurationNanos))})
		case columnPathRootSpanName:
			finalSpanset.RootSpanName = unsafeToString(e.Value.Bytes())
			c.traceAttrs = append(c.traceAttrs, attrVal{traceql.IntrinsicTraceRootSpanAttribute, traceql.NewStaticString(finalSpanset.RootSpanName)})
		case columnPathRootServiceName:
			finalSpanset.RootServiceName = unsafeToString(e.Value.Bytes())
			c.traceAttrs = append(c.traceAttrs, attrVal{traceql.IntrinsicTraceRootServiceAttribute, traceql.NewStaticString(finalSpanset.RootServiceName)})
		}
	}

	// Pre-allocate the final number of spans and serviceStats
	numSpans := 0
	numServiceStats := 0
	for _, e := range res.OtherEntries {
		if _, ok := e.Value.(*span); ok {
			numSpans++
		} else if _, ok := e.Value.(traceql.ServiceStats); ok {
			numServiceStats++
		}
	}
	if cap(finalSpanset.Spans) < numSpans {
		finalSpanset.Spans = make([]traceql.Span, 0, numSpans)
	}
	for _, e := range res.OtherEntries {
		if span, ok := e.Value.(*span); ok {
			finalSpanset.Spans = append(finalSpanset.Spans, span)
		}
	}

	// loop over all spans and add the trace-level attributes
	if len(c.traceAttrs) > 0 {
		for _, s := range finalSpanset.Spans {
			s := s.(*span)
			s.setTraceAttrs(c.traceAttrs)
		}
	}

	if numServiceStats > 0 {
		finalSpanset.ServiceStats = make(map[string]traceql.ServiceStats, numServiceStats)
		for _, e := range res.OtherEntries {
			if serviceStats, ok := e.Value.(traceql.ServiceStats); ok {
				finalSpanset.ServiceStats[e.Key] = serviceStats
			}
		}
	}

	res.Entries = res.Entries[:0]
	res.OtherEntries = res.OtherEntries[:0]
	res.AppendOtherValue(otherEntrySpansetKey, finalSpanset)

	return true
}

// serviceStatsCollector receives rows from the service stats
// columns and joins them together into map[string]ServiceStats entries.
type serviceStatsCollector struct{}

var _ parquetquery.GroupPredicate = (*serviceStatsCollector)(nil)

func (c *serviceStatsCollector) String() string {
	return "serviceStatsCollector{}"
}

func (c *serviceStatsCollector) KeepGroup(res *parquetquery.IteratorResult) bool {
	var key string
	var stats traceql.ServiceStats

	for _, e := range res.Entries {
		switch e.Key {
		case columnPathServiceStatsServiceName:
			key = e.Value.String()
		case columnPathServiceStatsSpanCount:
			stats.SpanCount = e.Value.Uint32()
		case columnPathServiceStatsErrorCount:
			stats.ErrorCount = e.Value.Uint32()
		}
	}

	res.Entries = res.Entries[:0]
	res.OtherEntries = res.OtherEntries[:0]
	res.AppendOtherValue(key, stats)

	return true
}

// attributeCollector receives rows from the individual key/string/int/etc
// columns and joins them together into map[key]value entries with the
// right type.
type attributeCollector struct {
	strBuffer   []string
	intBuffer   []int
	floatBuffer []float64
	boolBuffer  []bool
}

var _ parquetquery.GroupPredicate = (*attributeCollector)(nil)

func (c *attributeCollector) String() string {
	return "attributeCollector{}"
}

func (c *attributeCollector) KeepGroup(res *parquetquery.IteratorResult) bool {
	var key string
	var val traceql.Static

	// Reset buffers to reuse them without reallocating
	c.strBuffer = c.strBuffer[:0]
	c.intBuffer = c.intBuffer[:0]
	c.floatBuffer = c.floatBuffer[:0]
	c.boolBuffer = c.boolBuffer[:0]

	for _, e := range res.Entries {
		// Ignore nulls, this leaves val as the remaining found value,
		// or nil if the key was found but no matching values
		if e.Value.Kind() < 0 {
			continue
		}
		switch e.Key {
		case "key":
			key = unsafeToString(e.Value.Bytes())

		case "string":
			c.strBuffer = append(c.strBuffer, unsafeToString(e.Value.Bytes()))
		case "int":
			c.intBuffer = append(c.intBuffer, int(e.Value.Int64()))
		case "float":
			c.floatBuffer = append(c.floatBuffer, e.Value.Double())
		case "bool":
			c.boolBuffer = append(c.boolBuffer, e.Value.Boolean())
		}
	}

	// TODO: maybe pull IsArray here, and decide that to see if we have an array or not and make this go faster
	switch {
	// keep len == 1 cases first so we short-circuit early for non-array case
	case len(c.strBuffer) == 1:
		val = traceql.NewStaticString(c.strBuffer[0])
	case len(c.intBuffer) == 1:
		val = traceql.NewStaticInt(c.intBuffer[0])
	case len(c.floatBuffer) == 1:
		val = traceql.NewStaticFloat(c.floatBuffer[0])
	case len(c.boolBuffer) == 1:
		val = traceql.NewStaticBool(c.boolBuffer[0])
	case len(c.strBuffer) > 1:
		val = traceql.NewStaticStringArray(c.strBuffer)
	case len(c.intBuffer) > 1:
		val = traceql.NewStaticIntArray(c.intBuffer)
	case len(c.floatBuffer) > 1:
		val = traceql.NewStaticFloatArray(c.floatBuffer)
	case len(c.boolBuffer) > 1:
		val = traceql.NewStaticBooleanArray(c.boolBuffer)
	}

	// reset the slices
	res.Entries = res.Entries[:0]
	res.OtherEntries = res.OtherEntries[:0]
	res.AppendOtherValue(key, val)

	return true
}

type event struct {
	attrs []attrVal
}

var eventPool = sync.Pool{
	New: func() interface{} {
		return &event{}
	},
}

func putEvent(e *event) {
	e.attrs = e.attrs[:0]
	eventPool.Put(e)
}

func getEvent() *event {
	return eventPool.Get().(*event)
}

// eventCollector receives rows from the event columns and joins them together into
// map[key]value entries with the right type.
type eventCollector struct {
	minAttributes int
}

var _ parquetquery.GroupPredicate = (*eventCollector)(nil)

func (c *eventCollector) String() string {
	return "eventCollector{}"
}

func (c *eventCollector) KeepGroup(res *parquetquery.IteratorResult) bool {
	var ev *event

	// look for existing event first
	for _, e := range res.OtherEntries {
		if v, ok := e.Value.(*event); ok {
			ev = v
			break
		}
	}

	// if not found create a new one
	if ev == nil {
		ev = getEvent()
	}

	// extract from attribute collector
	for _, e := range res.OtherEntries {
		if v, ok := e.Value.(traceql.Static); ok {
			ev.attrs = append(ev.attrs, attrVal{
				a: newEventAttr(e.Key),
				s: v,
			})
		}
	}

	for _, e := range res.Entries {
		switch e.Key {
		case columnPathEventName:
			ev.attrs = append(ev.attrs, attrVal{
				a: traceql.IntrinsicEventNameAttribute,
				s: traceql.NewStaticString(unsafeToString(e.Value.Bytes())),
			})
		case columnPathEventTimeSinceStart:
			ev.attrs = append(ev.attrs, attrVal{
				a: traceql.IntrinsicEventTimeSinceStartAttribute,
				s: traceql.NewStaticDuration(time.Duration(e.Value.Int64())),
			})
		}
	}

	if c.minAttributes > 0 {
		if len(ev.attrs) < c.minAttributes {
			putEvent(ev)
			return false
		}
	}

	res.Reset()
	res.AppendOtherValue(otherEntryEventKey, ev)

	return true
}

type link struct {
	attrs []attrVal
}

var linkPool = sync.Pool{
	New: func() interface{} {
		return &link{}
	},
}

func putLink(l *link) {
	l.attrs = l.attrs[:0]
	linkPool.Put(l)
}

func getLink() *link {
	return linkPool.Get().(*link)
}

// linkCollector receives rows from the link columns and joins them together into
// map[key]value entries with the right type.
type linkCollector struct {
	minAttributes int
}

var _ parquetquery.GroupPredicate = (*linkCollector)(nil)

func (c *linkCollector) String() string {
	return "linkCollector{}"
}

func (c *linkCollector) KeepGroup(res *parquetquery.IteratorResult) bool {
	var l *link

	// look for existing link first
	for _, e := range res.OtherEntries {
		if v, ok := e.Value.(*link); ok {
			l = v
			break
		}
	}

	// if not found create a new one
	if l == nil {
		l = getLink()
	}

	// extract from attribute collector
	for _, e := range res.OtherEntries {
		if v, ok := e.Value.(traceql.Static); ok {
			l.attrs = append(l.attrs, attrVal{
				a: newLinkAttr(e.Key),
				s: v,
			})
		}
	}

	for _, e := range res.Entries {
		switch e.Key {
		case columnPathLinkTraceID:
			l.attrs = append(l.attrs, attrVal{
				a: traceql.NewIntrinsic(traceql.IntrinsicLinkTraceID),
				s: traceql.NewStaticString(util.TraceIDToHexString(e.Value.Bytes())),
			})
		case columnPathLinkSpanID:
			l.attrs = append(l.attrs, attrVal{
				a: traceql.NewIntrinsic(traceql.IntrinsicLinkSpanID),
				s: traceql.NewStaticString(util.SpanIDToHexString(e.Value.Bytes())),
			})
		}
	}

	if c.minAttributes > 0 {
		if len(l.attrs) < c.minAttributes {
			putLink(l)
			return false
		}
	}

	res.Reset()
	res.AppendOtherValue(otherEntryLinkKey, l)

	return true
}

func newSpanAttr(name string) traceql.Attribute {
	return traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, name)
}

func newResAttr(name string) traceql.Attribute {
	return traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, name)
}

func newInstrumentationAttrs(name string) traceql.Attribute {
	return traceql.NewScopedAttribute(traceql.AttributeScopeInstrumentation, false, name)
}

func newEventAttr(name string) traceql.Attribute {
	return traceql.NewScopedAttribute(traceql.AttributeScopeEvent, false, name)
}

func newLinkAttr(name string) traceql.Attribute {
	return traceql.NewScopedAttribute(traceql.AttributeScopeLink, false, name)
}

func unionIfNeeded(definitionLevel int, iters []parquetquery.Iterator, pred parquetquery.GroupPredicate) parquetquery.Iterator {
	switch len(iters) {
	case 0:
		return nil
	case 1:
		return iters[0]
	default:
		return parquetquery.NewUnionIterator(definitionLevel, iters, pred)
	}
}

func orIfNeeded(preds []parquetquery.Predicate) parquetquery.Predicate {
	switch len(preds) {
	case 0:
		return nil
	case 1:
		return preds[0]
	default:
		return parquetquery.NewOrPredicate(preds...)
	}
}

// unsafeToString casts a byte slice to a string w/o allocating
func unsafeToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func otlpStatusToTraceqlStatus(v uint64) traceql.Status {
	// Map OTLP status code back to TraceQL enum.
	// For other values, use the raw integer.
	switch v {
	case uint64(v1.Status_STATUS_CODE_UNSET):
		return traceql.StatusUnset
	case uint64(v1.Status_STATUS_CODE_OK):
		return traceql.StatusOk
	case uint64(v1.Status_STATUS_CODE_ERROR):
		return traceql.StatusError
	default:
		return traceql.Status(v)
	}
}

func otlpKindToTraceqlKind(v uint64) traceql.Kind {
	switch v {
	case uint64(v1.Span_SPAN_KIND_UNSPECIFIED):
		return traceql.KindUnspecified
	case uint64(v1.Span_SPAN_KIND_INTERNAL):
		return traceql.KindInternal
	case uint64(v1.Span_SPAN_KIND_SERVER):
		return traceql.KindServer
	case uint64(v1.Span_SPAN_KIND_CLIENT):
		return traceql.KindClient
	case uint64(v1.Span_SPAN_KIND_PRODUCER):
		return traceql.KindProducer
	case uint64(v1.Span_SPAN_KIND_CONSUMER):
		return traceql.KindConsumer
	default:
		return traceql.Kind(v)
	}
}
