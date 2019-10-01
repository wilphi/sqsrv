package sqtables

import (
	"sort"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
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
	log.Printf("Where expression = %s", whereExpr.ToString())
	var joins []JoinTable
	var joined []JoinTable

	for _, tabInfo := range tl.tables {
		log.Infof("Filtering table %s", tabInfo.TableName)

		// get the cols in the whereExpr
		cols := whereExpr.ColDefs(tabInfo.Table)
		if cols != nil {
			sort.Slice(cols, func(i, j int) bool { return cols[i].Idx < cols[j].Idx })
			i := 0
			for i < len(cols)-1 {
				if cols[i] == cols[i+1] {
					cols = append(cols[:i], cols[i+1:]...)
				}
				i++
			}
		} else {
			cols = make([]ColDef, 1)
			cols[0] = tabInfo.Table.tableCols[0]
		}
		whereList = ColsToExpr(NewColListDefs(cols))

		// Get the pointers to the rows based on the conditions
		tmpData, err := tabInfo.Table.GetRowData(profile, whereList, whereExpr)
		if err != nil {
			return nil, err
		}
		resultRows := make([]JoinRow, len(tmpData.Ptrs))
		for i, ptr := range tmpData.Ptrs {
			resultRows[i].Ptr = ptr
			resultRows[i].Vals = tmpData.Vals[i]
			resultRows[i].TableName = tabInfo.TableName
		}
		jt := JoinTable{Name: tabInfo.TableName, Tab: tabInfo.Table, Cols: cols, Rows: resultRows}
		joins = append(joins, jt)
	}
	sort.Slice(joins, func(i, j int) bool {
		return len(joins[i].Rows) < len(joins[j].Rows)
	})

	// Join the datasets together
	jtab := joins[0]
	joins = joins[1:]
	joined = append(joined, jtab)
	jresult := make([][]RowInterface, len(jtab.Rows))
	for i, r := range jtab.Rows {
		tmp := r
		jresult[i] = []RowInterface{&tmp}
	}

	var validJoin bool
	var lCol, rCol ColDef
	var idx int
	for len(joins) > 0 {
		var intermresult [][]RowInterface
		joinIdx := -1
		oldIdx := -1
		// find the smallest table that has a join expression, otherwise just use the smallest table
		for i, jt := range joins {
			// find the join clause
			validJoin, lCol, rCol, idx = findJoin(whereExpr, joined, jt.Name)

			if validJoin {
				log.Infof("Joining Expr %s = %s", lCol.ToString(), rCol.ToString())
				joinIdx = i
				oldIdx = idx
				break
			}
		}
		if joinIdx != -1 && oldIdx != -1 {
			// inner join the tables
			table1 := joined[oldIdx]
			table2 := joins[joinIdx]
			log.Printf("Joining %s - %s", table1.Name, table2.Name)
			col1Idx := findCol(table1.Cols, lCol)
			col2Idx := findCol(table2.Cols, rCol)
			log.Printf("Joining cols %s.%s : %s.%s", table1.Name, table1.Cols[col1Idx].ColName, table2.Name, table2.Cols[col2Idx].ColName)
			// Sort col
			sort.Slice(table2.Rows, func(i, j int) bool { return table2.Rows[i].Vals[col2Idx].LessThan(table2.Rows[j].Vals[col2Idx]) })
			for _, tuple := range jresult {
				leftVal, err := tuple[oldIdx].GetIdxVal(profile, col1Idx)
				if err != nil {
					return nil, err
				}
				rowIdx := sort.Search(len(table2.Rows), func(i int) bool { return !table2.Rows[i].Vals[col2Idx].LessThan(leftVal) })
				if rowIdx < len(table2.Rows) { //&& table2.Rows[rowIdx].Vals[col2Idx].Equal(leftVal) {
					for table2.Rows[rowIdx].Vals[col2Idx].Equal(leftVal) {
						tmpRow := table2.Rows[rowIdx]
						newTup := append(tuple, &tmpRow)
						intermresult = append(intermresult, newTup)
						rowIdx++
					}

				}
			}
			jresult = intermresult
			jtab := joins[joinIdx]
			joins = append(joins[:joinIdx], joins[joinIdx+1:]...)
			joined = append(joined, jtab)
			log.Printf("Join resulted in %d rows", len(jresult))
		}
		if joinIdx == -1 {
			table2 := joins[0]
			cnt := 0
			log.Printf("No valid join expression, executing cross join with table %s: creates %d rows", jtab.Name, len(jresult)*len(table2.Rows))
			for _, tuple := range jresult {
				for _, row := range table2.Rows {
					cnt++
					if cnt%1000000 == 0 {
						log.Print(cnt)
					}
					tmpRow := row
					newTup := append(tuple, &tmpRow)
					intermresult = append(intermresult, newTup)
				}
			}
			jresult = intermresult
			joins = joins[1:]
			joined = append(joined, table2)

		}

	}

	ret := NewExprDataSet(tl, eList)
	if eList.HasCount() {
		ret.Vals = make([][]sqtypes.Value, 1)
		ret.Vals[0] = make([]sqtypes.Value, eList.Len())
		ret.Vals[0][0] = sqtypes.NewSQInt(len(jresult))
		return ret, nil
	}
	ret.Vals = make([][]sqtypes.Value, len(jresult))

	for i, tuple := range jresult {
		rows := make([]RowInterface, len(joined))
		for j, tab := range joined {
			row, ok := tab.Tab.rowm[tuple[j].GetPtr(profile)]
			if !ok {
				return nil, sqerr.Newf("Invalid pointer for table %s:%d", tab.Name, tuple[j])
			}
			rows[j] = RowInterface(row)
		}
		ret.Vals[i], err = eList.Evaluate(profile, EvalPartial, rows...)
	}

	return ret, nil

}

func findCol(a []ColDef, b ColDef) int {
	for i, col := range a {
		if col == b {
			return i
		}
	}
	return -1
}
func findJoin(whereExpr Expr, joinedTables []JoinTable, tableName string) (validJoin bool, lColDef, rColDef ColDef, joinidx int) {
	tableName = strings.ToLower(tableName)
	opExprs := findOps(whereExpr, "=")
	tnames := make([]string, len(joinedTables))
	for i, jt := range joinedTables {
		tnames[i] = jt.Name
	}
	for _, ex := range opExprs {
		lEx := ex.Left()
		rEx := ex.Right()
		if lEx == nil || rEx == nil {
			continue
		}
		lColExpr, lok := lEx.(*ColExpr)
		rColExpr, rok := rEx.(*ColExpr)
		if lok && rok {
			lidx := contain(tnames, lColExpr.col.TableName)
			if lidx != -1 && rColExpr.col.TableName == tableName {
				return true, lColExpr.col, rColExpr.col, lidx
			}
			ridx := contain(tnames, rColExpr.col.TableName)
			if ridx != -1 && lColExpr.col.TableName == tableName {
				return true, rColExpr.col, lColExpr.col, ridx
			}
		}
	}
	return false, ColDef{}, ColDef{}, -1
}

func contain(a []string, str string) int {
	for idx, val := range a {
		if val == str {
			return idx
		}
	}
	return -1
}
func findOps(whereExpr Expr, op string) (ret []Expr) {
	var lOk, rOk bool
	lEx := whereExpr.Left()
	rEx := whereExpr.Right()

	opex, ok := whereExpr.(*OpExpr)
	if ok {
		if lEx != nil {
			_, lOk = lEx.(*ColExpr)
		}
		if rEx != nil {
			_, rOk = rEx.(*ColExpr)
		}
		if lOk && rOk && opex.Operator == op {
			ret = append(ret, opex)
		}

	}
	if lEx != nil {
		tRet := findOps(lEx, op)
		if tRet != nil {
			ret = append(ret, tRet...)
		}
	}
	if rEx != nil {
		tRet := findOps(rEx, op)
		if tRet != nil {
			ret = append(ret, tRet...)
		}
	}
	return
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
