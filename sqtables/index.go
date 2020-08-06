package sqtables

import (
	"fmt"
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtypes"
)

type idxElem struct {
	V   []sqtypes.Value
	Ptr sqptr.SQPtr
}

type elementArray []idxElem

// SQIndex -
type SQIndex struct {
	name        string
	elemArray   elementArray
	colHasNulls bool
	allowNulls  bool
	isUnique    bool
	cols        []column.Ref
}

func (ea elementArray) Len() int {
	return len(ea)
}
func (ea elementArray) Swap(i, j int) {
	ea[i], ea[j] = ea[j], ea[i]
}
func (ea elementArray) Less(i, j int) bool {
	x := 0
	for {
		if x >= len(ea[i].V) {
			// all values are equal
			break
		}
		if ea[i].V[x].LessThan(ea[j].V[x]) {
			return true
		}
		if ea[i].V[x].GreaterThan(ea[j].V[x]) {
			return false
		}
		// equal so test next level
		x++
	}
	return false
}

// AddElement -
//func (idx *SQIndex) AddElement(v []sqtypes.Value, ptr int) error {
func (idx *SQIndex) AddElement(d *DataSet) error {

	// make sure each index col is in the dataset
	dColList := d.GetColList()
	colMap := make([]int, len(idx.cols))
	for i, col := range idx.cols {
		cref := dColList.FindRef(col.ColName)
		if cref == nil || cref.Idx < 0 {
			return sqerr.Newf("Index column %q was not found in DataSet", col.ColName)

		}
		colMap[i] = cref.Idx
	}

	for i := range d.Vals {
		// values type must match index
		for j := range idx.cols {
			if idx.cols[j].ColType != d.Vals[i][j].Type() {
				return sqerr.New("Values added to index must match Column type")
			}
		}
		idx.elemArray = append(idx.elemArray, idxElem{V: d.Vals[i], Ptr: d.Ptrs[i]})
	}
	sort.Sort(idx.elemArray)
	return nil
}

// ToString -
func (idx SQIndex) String() string {
	str := fmt.Sprintf("Index %s (", idx.name)
	for _, c := range idx.cols {
		str += fmt.Sprintf("%s, ", c.ColName)
	}
	str = strings.TrimRight(str, ", ") + ")\n"

	for _, e := range idx.elemArray {
		str += "\t"
		for _, i := range e.V {
			str += "\t" + i.String()
		}
		str += fmt.Sprintf(" \t|| Pointer: %8d\n", e.Ptr)
	}
	return str
}

// IsSorted - used for testing
func (idx SQIndex) IsSorted() bool {
	return sort.IsSorted(idx.elemArray)
}

// NewSQIndex - Create a new Index for the given table
func NewSQIndex(profile *sqprofile.SQProfile, name string, tab *TableDef, cols *column.List, allowNulls, isUnique bool) (*SQIndex, error) {

	tabs := NewTableListFromTableDef(profile, tab)

	// Make sure that all columns in list exist in table
	err := cols.Validate(profile, tabs)
	if err != nil {
		return nil, err
	}

	idx := SQIndex{name: name, cols: cols.GetRefs(), colHasNulls: false, allowNulls: allowNulls, isUnique: isUnique}

	colMap := make(map[string]bool)

	// Make sure that a col is used only once
	for _, c := range cols.GetRefs() {
		if colMap[c.ColName] {
			return nil, sqerr.Newf("Create Index: Column %s can not occur more than once in index %s", c.ColName, name)
		}
		colMap[c.ColName] = true
	}
	tab.Lock(profile)
	defer tab.Unlock(profile)
	// Get the initial data
	for _, row := range tab.rowm {
		if !row.IsDeleted(profile) {
			vals := []sqtypes.Value{}
			for j, col := range idx.cols {
				vals[j], err = row.ColVal(profile, &col)
				if err != nil {
					return nil, err
				}
				idx.colHasNulls = idx.colHasNulls || vals[j].IsNull()
				if !allowNulls && idx.colHasNulls {
					return nil, sqerr.New("Index Creation: Null values are not allowed")
				}
			}
			idx.elemArray = append(idx.elemArray, idxElem{V: vals, Ptr: row.GetPtr(profile)})

		}
	}

	// sort the index
	sort.Sort(idx.elemArray)

	// verify unique if necessary
	if idx.isUnique {
		if len(idx.elemArray) > 1 {
			for i := 0; i < len(idx.elemArray)-1; i++ {
				e1 := idx.elemArray[i]
				e2 := idx.elemArray[i+1]
				match := true
				for j := range e1.V {
					if !e1.V[j].Equal(e2.V[j]) {
						match = false
					}
				}
				if match {
					// e1 = e2 so index is not unique
					return nil, sqerr.New("Index Creation - Index is not unique")
				}
			}
		}
	}
	fmt.Println("Created " + idx.String())
	log.Info("Created " + idx.String())
	return &idx, nil
}

/*
// NewSQIndex - Create a new Index based on dataset
func NewSQIndex(profile *sqprofile.SQProfile, name string, dataSet *DataSet) (*SQIndex, error) {
	var tab *TableRef
	// Unpack required data from the DataSet
	tabs := dataSet.GetTables()
	if tabs.Len() != 1 {
		return nil, sqerr.New("Indexes can only be created on a single table")
	}
	//Get the tableRef
	for _, table := range tabs.tables {
		tab = table
	}

	colList := dataSet.GetColList()
	col := colList.GetRefs()

	// Make sure that all columns in list exist in table
	err := colList.Validate(profile, tabs)
	if err != nil {
		return nil, err
	}

	idx := SQIndex{name: name, col: col, colHasNulls: false}

	colMap := make(map[string]bool)

	for _, c := range col {
		if ret, _ := tabs.FindDef(profile, c.ColName, ""); ret == nil {
			return nil, sqerr.Newf("Create Index: Column %s does not exist in table %s", c.ColName, tab.Name.String())
		}
		if colMap[c.ColName] {
			return nil, sqerr.Newf("Create Index: Column %s can not occur more than once in index %s", c.ColName, name)
		}
		colMap[c.ColName] = true
	}
	if dataSet.Vals == nil || dataSet.Ptrs == nil {

		return nil, sqerr.NewInternal("ColData and ColPtr do not match. At least one is nil")
	}
	if len(dataSet.Vals) != len(dataSet.Ptrs) {
		return nil, sqerr.NewInternal("length of Col data and pointer arrays do not match")
	}

	for i, row := range dataSet.Vals {
		for _, val := range row {
			if val.Type() == tokens.Null {
				// Null values cannot be indexed
				idx.colHasNulls = true
				break
			}
		}
		elem := idxElem{V: row, Ptr: dataSet.Ptrs[i]}
		idx.elemArray = append(idx.elemArray, elem)

	}
	fmt.Println(idx.ToString())

	// sort the index
	sort.Sort(idx.elemArray)
	str := ""
	for _, c := range col {
		str += c.ColName + ", "
	}
	str = strings.TrimRight(str, ", ")
	log.Infof("Index %q created on table %s column(s) %s", name, tab.Name, str)
	fmt.Println(idx.ToString())
	return &idx, nil
}

*/
