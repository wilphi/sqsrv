package sqtables

import (
	"sort"

	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

// DataSet - structure that contains a row/column set including column definitions
type DataSet struct {
	//cols       ColList
	Vals       [][]sqtypes.Value
	usePtrs    bool
	Ptrs       sqptr.SQPtrs
	tables     *TableList
	order      []OrderItem
	validOrder bool
	eList      *ExprList
	groupBy    *ExprList
}

// OrderItem stores information for ORDER BY clause
type OrderItem struct {
	ColName  string
	SortType tokens.TokenID
	idx      int
}

// GetColNames - returns a string array of column names
func (d *DataSet) GetColNames() []string {

	return d.eList.GetNames()
}

// NewDataSet creates a dataset based on a list of expressions
func NewDataSet(profile *sqprofile.SQProfile, tables *TableList, eList *ExprList, groupBy *ExprList) (*DataSet, error) {
	var err error

	if eList == nil || eList.Len() == 0 {
		return nil, sqerr.NewInternal("Expression List is empty for new DataSet")
	}
	// Verify all cols exist in table list
	if err = eList.ValidateCols(profile, tables); err != nil {
		return nil, err
	}

	if groupBy == nil && eList.HasAggregateFunc() {
		// make sure they are all aggregate funcs
		for _, exp := range eList.GetExprs() {
			fexp, ok := exp.(*FuncExpr)
			if ok && fexp.IsAggregate() {
				continue
			}
			return nil, sqerr.NewSyntax("Select Statements with Aggregate functions (count, sum, min, max, avg) must not have other expressions")
		}
	}
	// verify all groupby cols exist in table list
	if groupBy != nil {
		if err = groupBy.ValidateCols(profile, tables); err != nil {
			return nil, err
		}

		for _, exp := range groupBy.GetExprs() {
			if eList.FindName(exp.Name()) == -1 {
				return nil, sqerr.NewSyntaxf("%s is not in the expression list: %s", exp.Name(), eList.String())
			}
		}
		for _, exp := range eList.exprlist {
			fexp, ok := exp.(*FuncExpr)
			if ok {
				if !fexp.IsAggregate() {
					return nil, sqerr.NewSyntaxf("%s is not an aggregate function", fexp.Name())
				}
				continue
			}
			if groupBy.FindName(exp.Name()) == -1 {
				return nil, sqerr.NewSyntaxf("%s is not in the group by clause: %v", exp.Name(), groupBy.String())
			}
		}
		eList.ValidateCols(profile, tables)
	}

	return &DataSet{eList: eList, tables: tables, groupBy: groupBy}, nil
}

// NumCols -
func (d *DataSet) NumCols() int {
	return d.eList.Len()
}

// GetColList -
func (d *DataSet) GetColList() *ColList {
	cols := make([]ColDef, d.eList.Len())
	for i, ex := range d.eList.exprlist {
		cols[i] = ex.ColDef()
	}
	return NewColListDefs(cols)
}

// GetTables -
func (d *DataSet) GetTables() *TableList {
	return d.tables
}

// SetOrder -
func (d *DataSet) SetOrder(order []OrderItem) error {
	d.validOrder = false
	d.order = order
	for x, col := range d.order {
		//set the index
		d.order[x].idx = d.eList.FindName(col.ColName)
		if d.order[x].idx < 0 {
			// Col not found
			return sqerr.Newf("Column %s not found in dataset", col.ColName)
		}
	}
	d.validOrder = true
	return nil
}

// Len - used for sorting
func (d *DataSet) Len() int {
	if d.Vals == nil {
		return 0
	}
	return len(d.Vals)

}

// Swap - used for sorting
func (d *DataSet) Swap(i, j int) {
	d.Vals[i], d.Vals[j] = d.Vals[j], d.Vals[i]
}

// Less is part of sort Interface
func (d *DataSet) Less(i, j int) bool {
	if len(d.order) > 0 {
		for x := range d.order {
			col := d.order[x]
			nullA := d.Vals[i][col.idx] == nil || d.Vals[i][col.idx].IsNull()
			nullB := d.Vals[j][col.idx] == nil || d.Vals[j][col.idx].IsNull()
			if nullA && nullB {
				continue
			}
			if d.Vals[i][col.idx].LessThan(d.Vals[j][col.idx]) || nullB {
				return col.SortType == tokens.Asc
			}
			if d.Vals[i][col.idx].GreaterThan(d.Vals[j][col.idx]) || nullA {
				return col.SortType != tokens.Asc
			}
		}
	} else {
		for x := 0; x < d.eList.Len(); x++ {
			nullA := d.Vals[i][x].IsNull()
			nullB := d.Vals[j][x].IsNull()
			if nullA && nullB {
				continue
			}
			if d.Vals[i][x].LessThan(d.Vals[j][x]) || nullB {
				return true
			}
			if d.Vals[i][x].GreaterThan(d.Vals[j][x]) || nullA {
				return false
			}
		}
	}
	return true
}

// Distinct sorts and removes duplicate rows in the data set
func (d *DataSet) Distinct() {
	sort.Sort(d)
	if (len(d.Vals) - 1) > 0 {
		tmp := d.Vals[:1]
		for i := 0; i < len(d.Vals)-1; i++ {
			match := false
			for j := 0; j < len(d.Vals[i]); j++ {
				if d.Vals[i][j].Equal(d.Vals[i+1][j]) {
					match = true
				} else {
					match = false
					break
				}
			}
			if !match {
				tmp = append(tmp, d.Vals[i+1])
			}
		}
		d.Vals = tmp
	}
}

// Sort is a convenience function
func (d *DataSet) Sort() error {
	if len(d.order) <= 0 || !d.validOrder {
		return sqerr.New("Sort Order has not been set for DataSet")
	}

	sort.Sort(d)
	return nil
}

// GroupBy sorts and removes duplicate rows in the data set
func (d *DataSet) GroupBy() error {
	var err error
	var gbOrder []OrderItem

	funcEx, funcIdx := d.eList.FindAggregateFuncs()
	colCnt := make([]int, d.eList.Len())
	//sort by the group by Cols

	if d.groupBy != nil {
		//save the original order
		oldOrder := d.order
		defer func() {
			d.order = oldOrder
		}()
		d.order = nil

		// Set the sort order to be the same as the group by & Sort
		gbOrder = make([]OrderItem, d.groupBy.Len())
		for i, expr := range d.groupBy.GetExprs() {
			gbOrder[i] = OrderItem{ColName: expr.Name(), SortType: tokens.Asc}
		}
		err = d.SetOrder(gbOrder)
		if err != nil {
			return err
		}
		err = d.Sort()
		if err != nil {
			return err
		}
	}
	result := make([][]sqtypes.Value, 0)
	var res []sqtypes.Value
	grpCnt := 0
	resultIdx := 0
	var match bool
	for i := range d.Vals {
		if len(result) == resultIdx {
			if d.eList.Len() != len(d.Vals[i]) {
				return sqerr.NewInternalf("Expression list len (%d) does not match value list len (%d)", d.eList.Len(), len(d.Vals[i]))
			}
			res, colCnt = initResultRow(d.eList.Len(), d.Vals[i])
			result = append(result, res)
			grpCnt = 1

		} else {
			grpCnt++
			result[resultIdx], colCnt, err = calcAggregates(d.Vals[i], result[resultIdx], funcEx, funcIdx, colCnt)
		}
		match = d.groupBy == nil

		if d.groupBy != nil && i < len(d.Vals)-1 {
			for _, exp := range gbOrder {
				if d.Vals[i][exp.idx].Equal(d.Vals[i+1][exp.idx]) || (d.Vals[i][exp.idx].IsNull() && d.Vals[i+1][exp.idx].IsNull()) {
					match = true
				} else {
					match = false
					break
				}
			}
		}
		if !match {
			v, err := finalizeGroup(result[resultIdx], funcEx, funcIdx, grpCnt, colCnt)
			if err != nil {
				return nil
			}
			result[resultIdx] = v
			resultIdx++
			grpCnt = 0
		}
	}

	// fixup last row of results
	if match {
		res, err = finalizeGroup(result[resultIdx], funcEx, funcIdx, grpCnt, colCnt)
		if err != nil {
			return err
		}
		result[resultIdx] = res
	}
	d.Vals = result

	return nil
}

func finalizeGroup(valRow []sqtypes.Value, funcEx []*FuncExpr, funcIdx []int, mcnt int, colCnt []int) ([]sqtypes.Value, error) {

	for j, fex := range funcEx {
		switch fex.Cmd {
		case tokens.Count:
			valRow[funcIdx[j]] = sqtypes.NewSQInt(mcnt)
		case tokens.Avg:
			numer, err := valRow[funcIdx[j]].Convert(tokens.Float)
			if err != nil {
				return nil, err
			}
			denom := sqtypes.NewSQFloat(float64(colCnt[funcIdx[j]]))
			v, err := numer.Operation(tokens.Divide, denom)
			if err != nil {
				return nil, err
			}
			valRow[funcIdx[j]] = v
		}

	}
	return valRow, nil
}

func initResultRow(resultLen int, vals []sqtypes.Value) ([]sqtypes.Value, []int) {
	result := make([]sqtypes.Value, resultLen)
	colCnt := make([]int, resultLen)
	for i := range vals {
		result[i] = vals[i]
		if !result[i].IsNull() {
			colCnt[i] = 1
		}

	}
	return result, colCnt
}
func calcAggregates(vals, result []sqtypes.Value,
	funcEx []*FuncExpr,
	funcIdx []int,
	colCnt []int) ([]sqtypes.Value, []int, error) {
	var err error
	for j, fex := range funcEx {
		if fex.Cmd != tokens.Count {
			k := funcIdx[j]
			v := vals[k]
			if !v.IsNull() {
				if result[k] == nil || result[k].IsNull() {
					result[k] = v
					colCnt[k] = 1
				} else {
					switch fex.Cmd {
					case tokens.Sum:
						result[k], err = result[k].Operation(tokens.Plus, v)
					case tokens.Min:
						lt, err := v.Operation(tokens.LessThan, result[k])
						if err == nil {
							b, ok := lt.(sqtypes.SQBool)
							if b.Bool() && ok {
								result[k] = v
							}
						}
					case tokens.Max:
						lt, err := v.Operation(tokens.GreaterThan, result[k])
						if err == nil {
							b, ok := lt.(sqtypes.SQBool)
							if b.Bool() && ok {
								result[k] = v
							}
						}
					case tokens.Avg:
						result[k], err = result[k].Operation(tokens.Plus, v)
						colCnt[k]++
					default:
						return nil, nil, sqerr.NewInternalf("Function %s is not a valid aggregate function", tokens.IDName(fex.Cmd))
					}
					if err != nil {
						return nil, nil, err
					}
				}
			}

		}

	}

	return result, colCnt, nil
}
