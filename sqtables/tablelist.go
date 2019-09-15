package sqtables

import (
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtypes"
)

// FromTable structure holds the alias name, actual tableName and pointer to actual table
type FromTable struct {
	TableName string
	Alias     string
	Table     *TableDef
}

//TableList holds a unique list of tables listed in From Clause of query
type TableList struct {
	tables map[string]*FromTable
}

// FindTableDef - Find a table def given the table name/alias
func (tl *TableList) FindTableDef(profile *sqprofile.SQProfile, name string) *TableDef {
	ft, ok := tl.tables[strings.ToLower(name)]
	if !ok {
		return nil
	}
	return ft.Table
}

// ToString - generates a string containing the names of the tables in the list
func (tl *TableList) ToString(profile *sqprofile.SQProfile) string {
	return strings.Join(tl.TableNames(), ", ")
}

//FindColDef - Finds a ColDef based on colName and tableAlias. If the tableAlias is empty, it will look in all
//   tables in the list for the column. An error will occur if the col is found in multiple tables
func (tl *TableList) FindColDef(profile *sqprofile.SQProfile, colName, tableAlias string) (col *ColDef, err error) {

	if tableAlias == "" {
		found := false
		for _, ft := range tl.tables {
			if ft.Table == nil {
				return nil, sqerr.NewInternalf("Table %s does not have a TableDef assigned", ft.TableName)
			}
			cd := ft.Table.FindColDef(profile, colName)
			if cd != nil {
				if found {
					// has been found before
					return nil, sqerr.Newf("Column %q found in multiple tables, add tablename to differentiate", colName)
				}
				col = cd
				found = true
			}
		}
		if !found {
			return nil, sqerr.Newf("Column %q not found in Table(s): %s", colName, tl.ToString(profile))
		}
	} else {
		tabF, ok := tl.tables[strings.ToLower(tableAlias)]
		if !ok {
			return nil, sqerr.Newf("Table %s not found in table list", tableAlias)
		}
		tab := tabF.Table
		if tab == nil {
			return nil, sqerr.NewInternalf("Table %s does not have a TableDef", tableAlias)
		}
		col = tab.FindColDef(profile, colName)
		if col == nil {
			return nil, sqerr.Newf("Column %q not found in Table %q", colName, tableAlias)
		}
	}
	return col, nil
}

// Len returns number of tables in list
func (tl *TableList) Len() int {
	return len(tl.tables)
}

// Add  a new table to the list. Will return an error if a duplicate
// tableName/Alias pair is added. If the TableDef is not provided it will be found
func (tl *TableList) Add(profile *sqprofile.SQProfile, ft FromTable) error {

	if ft.Table == nil {
		// Get the TableDef
		ft.Table = GetTable(profile, ft.TableName)
		if ft.Table == nil {
			return sqerr.Newf("Table %q does not exist", ft.TableName)
		}
	}

	// if there is an alias use it as key instead of tableName
	key := strings.ToLower(Ternary(ft.Alias != "", ft.Alias, ft.TableName))

	//See if key has already been used
	_, ok := tl.tables[key]
	if ok {
		return sqerr.Newf("Duplicate table name/alias %q", key)
	}
	tl.tables[key] = &ft

	return nil
}

// AllCols returns an array of all cols in the tables of the tablelist as ColDefs
func (tl *TableList) AllCols(profile *sqprofile.SQProfile) []ColDef {

	var cols []ColDef
	colm := make(map[ColDef]bool)
	for _, tab := range tl.tables {
		tc := tab.Table.GetCols(profile)
		for _, cd := range tc.GetColDefs() {
			colm[cd] = true
		}
	}
	for key := range colm {
		cols = append(cols, key)
	}
	return cols
}

// RLock read locks all tables in the list
func (tl *TableList) RLock(profile *sqprofile.SQProfile) {
	for _, tab := range tl.tables {
		tab.Table.RLock(profile)
	}
}

// RUnlock unloack read lock from all tables in list
func (tl *TableList) RUnlock(profile *sqprofile.SQProfile) {
	for _, tab := range tl.tables {
		tab.Table.RUnlock(profile)
	}
}

// TableNames returns the tablenames of the TableList
func (tl *TableList) TableNames() []string {
	names := make([]string, len(tl.tables))
	i := 0
	for _, tab := range tl.tables {
		names[i] = tab.TableName
		i++
	}
	sort.Strings(names)
	return names
}

// GetRowData - Returns a dataset with the data from the tables
func (tl *TableList) GetRowData(profile *sqprofile.SQProfile, eList *ExprList, whereExpr Expr) (*DataSet, error) {
	var err error
	var whereList *ExprList

	tl.RLock(profile)
	defer tl.RUnlock(profile)

	if eList == nil || eList.Len() < 1 {
		return nil, sqerr.NewInternal("Expression List must have at least one item")
	}
	if tl.Len() == 0 {
		return nil, sqerr.NewInternal("TableList must not be empty in TableList.GetRowData")
	}
	if tl.Len() == 1 {
		// Single table query
		var tab *TableDef
		for _, tabInfo := range tl.tables {
			tab = tabInfo.Table
		}
		return tab.GetRowData(profile, eList, whereExpr)
	}

	if whereExpr == nil {
		return nil, sqerr.New("Multi table queries must have a valid where clause")
	}
	// Verify all cols exist in tables
	if err = eList.ValidateCols(profile, tl); err != nil {
		return nil, err
	}
	if err = whereExpr.ValidateCols(profile, tl); err != nil {
		return nil, err
	}

	nTab := 0
	tabs := make([]*FromTable, tl.Len())
	tmpData := make([]*DataSet, tl.Len())
	for _, tabInfo := range tl.tables {
		tabs[nTab] = tabInfo

		log.Warnf("Filtering table %s", tabInfo.TableName)

		// get the cols in the whereExpr
		cols := whereExpr.ColDefs(tabInfo.Table)
		whereList = ColsToExpr(NewColListDefs(cols))

		//		tmpData[nTab] = NewExprDataSet(tl, whereList)
		// Get the pointers to the rows based on the conditions
		tmpData[nTab], err = tabInfo.Table.GetRowData(profile, whereList, whereExpr)
		//ptrs, err := tabInfo.Table.GetRowPtrs(profile, whereExpr, RowOrder)
		if err != nil {
			return nil, err
		}
		//tmpData[nTab].Ptrs = ptrs
		nTab++
	}
	sort.Slice(tabs, func(i, j int) bool {
		return tmpData[i].Len() < tmpData[j].Len()
	})
	sort.Slice(tmpData, func(i, j int) bool {
		return tmpData[i].Len() < tmpData[j].Len()
	})
	// Join the datasets together
	var result [][]sqptr.SQPtr
	for i, tabInfo := range tabs {
		if result == nil {
			result = make([][]sqptr.SQPtr, len(tmpData[i].Ptrs))
			for i, ptr := range tmpData[i].Ptrs {
				result[i] = make(sqptr.SQPtrs, 1)
				result[i][0] = ptr
			}
			continue
		}
		var intermRes [][]sqptr.SQPtr

		log.Printf("%d rows to be processed to join table %s", len(result)*len(tmpData[i].Ptrs), tabInfo.TableName)
		cnt := 0
		for _, tuple := range result {
			for _, ptr := range tmpData[i].Ptrs {
				cnt++
				if cnt%1000000 == 0 {
					log.Print(cnt)
				}
				nTuple := append(tuple, ptr)
				// Validate tuple
				tupleRows := make([]*RowDef, len(nTuple))
				for k, p := range nTuple {
					tupleRows[k] = tabs[k].Table.GetRow(profile, p)
				}
				val, err := whereExpr.Evaluate(profile, EvalPartial, tupleRows...)
				if err != nil {
					return nil, err
				}
				includeTuple := false
				if val != nil {
					boolVal, ok := val.(sqtypes.SQBool)
					if ok {
						includeTuple = boolVal.Val
					}
				} else {
					includeTuple = true
				}
				if includeTuple {
					// Add tuple to results
					intermRes = append(intermRes, nTuple)
				}
			}
		}
		result = intermRes

	}
	ret := NewExprDataSet(tl, eList)
	if eList.HasCount() {
		ret.Vals = make([][]sqtypes.Value, 1)
		ret.Vals[0] = make([]sqtypes.Value, eList.Len())
		ret.Vals[0][0] = sqtypes.NewSQInt(len(result))
		return ret, nil
	}
	ret.Vals = make([][]sqtypes.Value, len(result))

	for i, tuple := range result {
		rows := make([]*RowDef, len(tabs))
		for j, tab := range tabs {
			row, ok := tab.Table.rowm[tuple[j]]
			if !ok {
				return nil, sqerr.Newf("Invalid pointer for table %s:%d", Ternary(tab.Alias == "", tab.TableName, tab.Alias+"("+tab.TableName+")"), tuple[j])
			}
			rows[j] = row
		}
		ret.Vals[i], err = eList.Evaluate(profile, EvalPartial, rows...)
	}

	return ret, nil

}

// NewTableList - Initialize a new TableList
func NewTableList(profile *sqprofile.SQProfile, tables []FromTable) *TableList {
	tl := TableList{tables: make(map[string]*FromTable)}
	for _, ft := range tables {
		tl.Add(profile, ft)
	}
	return &tl
}

// NewTableListFromTableDef - Initialize a new TableList
func NewTableListFromTableDef(profile *sqprofile.SQProfile, tabs ...*TableDef) *TableList {
	tl := TableList{tables: make(map[string]*FromTable)}
	for _, tab := range tabs {
		ft := FromTable{TableName: tab.GetName(profile), Table: tab}
		tl.Add(profile, ft)
	}

	return &tl
}

// Ternary is an implementation of a ternary operator for strings
func Ternary(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}
