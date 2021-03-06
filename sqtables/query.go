package sqtables

import (
	"sort"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/isdebug"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

// Query has all of the information required for a query
type Query struct {
	Tables     TableList
	EList      *ExprList
	IsDistinct bool
	WhereExpr  Expr
	GroupBy    *ExprList
	HavingExpr *Expr
	OrderBy    []OrderItem
	Joins      []JoinInfo
}

// JoinInfo contains the information required for a table join
type JoinInfo struct {
	TableA, TableB TableRef
	JoinType       tokens.TokenID
	ONClause       Expr
}

// GetRowData - Returns a dataset with the data from the tables
func (q *Query) GetRowData(profile *sqprofile.SQProfile) (*DataSet, error) {
	var err error
	var finalResult *DataSet
	var whereList *ExprList
	timeOut := new(int32)

	if q.EList == nil || q.EList.Len() < 1 {
		return nil, sqerr.NewInternal("Expression List must have at least one item")
	}
	if q.Tables.Len() == 0 {
		return nil, sqerr.NewInternal("TableList must not be empty for query")
	}

	err = q.Tables.RLock(profile)
	if err != nil {
		return nil, err
	}
	defer q.Tables.RUnlock(profile)

	// Setup a timer to idicate when the query has gone on too long
	maxTimer := time.NewTimer(time.Minute)
	defer maxTimer.Stop()
	go func() {
		<-maxTimer.C
		atomic.StoreInt32(timeOut, 1)
	}()

	// If we are debugging then disable the timer
	if isdebug.Enabled {
		maxTimer.Stop()
	}

	// Verify all cols exist in tables
	if err = q.EList.ValidateCols(profile, q.Tables); err != nil {
		return nil, err
	}

	// Validate Where clause
	if q.WhereExpr != nil {
		err = q.WhereExpr.ValidateCols(profile, q.Tables)
		if err != nil {
			return nil, err
		}
	}

	// Validate Joins
	for _, j := range q.Joins {
		if j.ONClause != nil {
			err = j.ONClause.ValidateCols(profile, q.Tables)
			if err != nil {
				return nil, err
			}
		}
	}

	// Make sure groupby, having clause and eList follow rules for group by (if there is one)
	err = q.ValidateGroupBySemantics(profile)
	if err != nil {
		return nil, err
	}

	// Setup the result dataset
	finalResult, err = NewDataSet(profile, q.Tables, q.EList)
	if err != nil {
		return nil, err
	}
	finalResult.usePtrs = !q.EList.HasAggregateFunc()

	// Added if statement to prevent the exection of String unless required
	if log.GetLevel() >= log.DebugLevel {
		log.Debugf("Where expression = %s", q.WhereExpr)
	}
	var unJoined []JoinTable
	var joined []JoinTable

	// Create a virtual Where clause with all of the ON expressions to get complete list of cols for each table
	vWhere := q.WhereExpr
	for _, join := range q.Joins {
		if join.ONClause != nil {
			if vWhere == nil {
				vWhere = join.ONClause
			} else {
				vWhere = NewOpExpr(vWhere, tokens.And, join.ONClause)
			}
		}
	}

	for _, tabInfo := range q.Tables {
		log.Debugf("Filtering table %s", tabInfo.Name)

		// get the cols in the virtual Where
		var cols []column.Ref
		if vWhere != nil {
			cols = vWhere.ColRefs(tabInfo.Name)
		}
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
			cols = make([]column.Ref, 1)
			cols[0] = tabInfo.Table.tableCols[0].Ref()
			cols[0].TableName = tabInfo.Name
		}
		whereList = ColsToExpr(column.NewListRefs(cols))

		// Get the pointers to the rows based on the conditions
		tmpData, err := tabInfo.GetRowData(profile, whereList, q.WhereExpr)
		if err != nil {
			return nil, err
		}
		resultRows := make([]JoinRow, len(tmpData.Ptrs))
		for i, ptr := range tmpData.Ptrs {
			resultRows[i].Ptr = ptr
			resultRows[i].Vals = tmpData.Vals[i]
			resultRows[i].TableName = tabInfo.Name
		}
		jt := JoinTable{TR: *tabInfo, Cols: cols, Rows: resultRows}
		unJoined = append(unJoined, jt)
		log.Debugf("Filtered %s %d rows", tabInfo.Name, len(resultRows))
	}
	// Sort tables from smallest to largest # rows returned
	sort.Slice(unJoined, func(i, j int) bool {
		return len(unJoined[i].Rows) < len(unJoined[j].Rows)
	})
	if atomic.LoadInt32(timeOut) != 0 {
		return nil, sqerr.New("Query terminated due to timeout")
	}
	// Join the datasets together
	jtab := unJoined[0]
	unJoined = unJoined[1:]
	joined = append(joined, jtab)
	jresult := make([][]RowInterface, len(jtab.Rows))
	for i, r := range jtab.Rows {
		tmp := r
		jresult[i] = []RowInterface{&tmp}
	}

	var joinIdx, joinedIdx, unJoinedIdx int
	unusedJoins := q.Joins // list of joins that have not already been used to join intermediate results
	for len(unJoined) > 0 {
		// find the join clause
		joinIdx, joinedIdx, unJoinedIdx = findJoin(unusedJoins, joined, unJoined)
		if joinIdx == -1 || joinedIdx == -1 || unJoinedIdx == -1 {
			return nil, sqerr.Newf("Could not find a valid join for %s", unJoined[0].TR.Name)
		}
		log.Debugf("Joining tables %s, %s using Expr %s ", unusedJoins[joinIdx].TableA.Name,
			unusedJoins[joinIdx].TableB.Name, unusedJoins[joinIdx].ONClause)

		currentJoin := unusedJoins[joinIdx]
		unusedJoins = append(unusedJoins[:joinIdx], unusedJoins[joinIdx+1:]...)

		switch currentJoin.JoinType {
		case tokens.Inner:
			jresult, err = innerJoin(profile, currentJoin, joined[joinedIdx], unJoined[unJoinedIdx], joinedIdx, jresult)
			if err != nil {
				return nil, err
			}
			log.Debugf("Join resulted in %d rows", len(jresult))
		case tokens.Cross:
			jresult, err = crossJoin(profile, currentJoin, joined[joinedIdx], unJoined[unJoinedIdx], joinedIdx, jresult, timeOut)
			if err != nil {
				return nil, err
			}
			log.Debugf("Cross Join resulted in %d rows", len(jresult))
		case tokens.Left, tokens.Right, tokens.Full:
			jresult, err = outerJoin(profile, joined, currentJoin, joined[joinedIdx], unJoined[unJoinedIdx], joinedIdx, jresult)
			if err != nil {
				return nil, err
			}
			log.Debugf("%s Outer Join resulted in %d rows", tokens.IDName(currentJoin.JoinType), len(jresult))
		default:
			return nil, sqerr.NewInternalf("Join Type %s is not currently implemented", tokens.IDName(currentJoin.JoinType))
		}
		jtab := unJoined[unJoinedIdx]
		unJoined = append(unJoined[:unJoinedIdx], unJoined[unJoinedIdx+1:]...)
		joined = append(joined, jtab)

	}

	// Fill in the final Datastore result
	finalResult.Vals = make([][]sqtypes.Value, len(jresult))

	for i, tuple := range jresult {
		rows := make([]RowInterface, len(joined))
		for j, tab := range joined {
			ptr := tuple[j].GetPtr(profile)
			// The ptr will be 0 in the case of an outer join. That table's results will be nulls
			if ptr != 0 {
				row, ok := tab.TR.Table.rowm[ptr]
				if !ok {
					return nil, sqerr.Newf("Invalid pointer for table %s:%d", tab.TR.Name, tuple[j])
				}
				rows[j] = RowInterface(row)
			} else {
				row := JoinRow{Vals: make([]sqtypes.Value, len(tab.TR.Table.tableCols)), TableName: tab.TR.Name}
				for x := range row.Vals {
					row.Vals[x] = sqtypes.NewSQNull()
				}
				rows[j] = RowInterface(&row)
			}
		}
		finalResult.Vals[i], err = q.EList.Evaluate(profile, EvalPartial, rows...)
		if err != nil {
			return nil, err
		}
	}
	if q.GroupBy != nil || q.EList.HasAggregateFunc() {
		err = q.ProcessGroupBy(profile, finalResult)
	}
	return finalResult, nil

}

func findCol(a []column.Ref, b column.Ref) int {
	for i, col := range a {
		if col == b {
			return i
		}
	}
	return -1
}

// findJoin finds a join that has a table in joined and a table in unjoined
func findJoin(joins []JoinInfo, joined, unjoined []JoinTable) (joinIdx int, joinedIdx int, unjoinedIdx int) {
	// find a join that joins a previously joined table to an unjoined table
	for i, tab := range joined {
		// look through the joins
		for x, join := range joins {
			if moniker.Equal(tab.TR.Name, join.TableA.Name) || moniker.Equal(tab.TR.Name, join.TableB.Name) {
				// look through the list of unjoined tables
				for j, ujtab := range unjoined {
					if moniker.Equal(ujtab.TR.Name, join.TableA.Name) || moniker.Equal(ujtab.TR.Name, join.TableB.Name) {
						// found a join that has a table in joined and a table in unjoined
						return x, i, j
					}
				}

			}
		}

	}
	// If the parser is doing its job properly, this function should never return -1, -1, -1
	return -1, -1, -1
}

//crossJoin
func crossJoin(profile *sqprofile.SQProfile, currentJoin JoinInfo, table1, table2 JoinTable, joinedIdx int,
	jresult [][]RowInterface, timeOut *int32) ([][]RowInterface, error) {
	var intermresult [][]RowInterface
	cnt := 0
	log.Debugf("Cross join with table %s: creates %d rows", table2.TR.Name, len(jresult)*len(table2.Rows))
	for _, tuple := range jresult {
		for _, row := range table2.Rows {
			cnt++
			if cnt%1000000 == 0 {
				log.Print(cnt)
				if atomic.LoadInt32(timeOut) != 0 {
					return nil, sqerr.New("Query terminated due to timeout")
				}
			}
			tmpRow := row
			newTup := append(tuple, &tmpRow)
			intermresult = append(intermresult, newTup)
		}
	}
	return intermresult, nil
}

//innerJoin
func innerJoin(profile *sqprofile.SQProfile, currentJoin JoinInfo, joinedTab, unJoinedTab JoinTable, joinedIdx int,
	jresult [][]RowInterface) ([][]RowInterface, error) {
	var intermresult [][]RowInterface
	// do inner join
	if currentJoin.ONClause == nil {
		return nil, sqerr.NewInternal("Missing ON Clause for inner join")
	}
	col1 := currentJoin.ONClause.ColRefs(joinedTab.TR.Name)
	col2 := currentJoin.ONClause.ColRefs(unJoinedTab.TR.Name)
	col1Idx := findCol(joinedTab.Cols, col1[0])
	col2Idx := findCol(unJoinedTab.Cols, col2[0])
	log.Debugf("Joining cols %s.%s : %s.%s", joinedTab.TR.Name, joinedTab.Cols[col1Idx].ColName, unJoinedTab.TR.Name, unJoinedTab.Cols[col2Idx].ColName)
	sort.Slice(unJoinedTab.Rows, func(i, j int) bool {
		return unJoinedTab.Rows[i].Vals[col2Idx].LessThan(unJoinedTab.Rows[j].Vals[col2Idx])
	})
	if isdebug.Enabled {
		s := ""
		for _, rw := range unJoinedTab.Rows {
			s += rw.Vals[col2Idx].String() + " "
		}
		log.Debugf("Sorted unJoinedTab Rows = %s", s)
	}
	for _, tuple := range jresult {
		leftVal, err := tuple[joinedIdx].IdxVal(profile, col1Idx)
		if err != nil {
			return nil, err
		}
		rowIdx := sort.Search(len(unJoinedTab.Rows), func(i int) bool { return !unJoinedTab.Rows[i].Vals[col2Idx].LessThan(leftVal) })
		for (rowIdx < len(unJoinedTab.Rows)) && unJoinedTab.Rows[rowIdx].Vals[col2Idx].Equal(leftVal) {
			tmpRow := unJoinedTab.Rows[rowIdx]
			newTup := append(tuple, &tmpRow)
			intermresult = append(intermresult, newTup)
			rowIdx++
		}
	}
	return intermresult, nil

}

//outerJoin
func outerJoin(profile *sqprofile.SQProfile, joined []JoinTable, currentJoin JoinInfo, joinedTab, unJoinedTab JoinTable, joinedIdx int,
	jresult [][]RowInterface) ([][]RowInterface, error) {
	var intermresult [][]RowInterface
	var rightMatch []bool

	// do outer join
	if currentJoin.ONClause == nil {
		return nil, sqerr.NewInternal("Missing ON Clause for outer join")
	}
	// Make sure the already joined tables are on the left
	if !moniker.Equal(currentJoin.TableA.Name, joinedTab.TR.Name) {
		currentJoin = swapOuterJoin(currentJoin)
	}
	isLeft := currentJoin.JoinType == tokens.Left || currentJoin.JoinType == tokens.Full
	isRight := currentJoin.JoinType == tokens.Right || currentJoin.JoinType == tokens.Full

	if !(isLeft || isRight) {
		return nil, sqerr.Newf("Unknown Outer Join type: %s", tokens.IDName(currentJoin.JoinType))
	}
	col1 := currentJoin.ONClause.ColRefs(joinedTab.TR.Name)
	col2 := currentJoin.ONClause.ColRefs(unJoinedTab.TR.Name)
	col1Idx := findCol(joinedTab.Cols, col1[0])
	col2Idx := findCol(unJoinedTab.Cols, col2[0])
	log.Debugf("%s Outer Join on cols %s.%s : %s.%s", tokens.IDName(currentJoin.JoinType), joinedTab.TR.Name, joinedTab.Cols[col1Idx].ColName, unJoinedTab.TR.Name, unJoinedTab.Cols[col2Idx].ColName)

	if isRight {
		rightMatch = make([]bool, len(unJoinedTab.Rows))
	}
	for _, tuple := range jresult {
		leftVal, err := tuple[joinedIdx].IdxVal(profile, col1Idx)
		if err != nil {
			return nil, err
		}
		match := false
		for rowIdx := range unJoinedTab.Rows {
			if unJoinedTab.Rows[rowIdx].Vals[col2Idx].Equal(leftVal) {
				tmpRow := unJoinedTab.Rows[rowIdx]
				newTup := append(tuple, &tmpRow)
				intermresult = append(intermresult, newTup)
				match = true
				if isRight {
					rightMatch[rowIdx] = true
				}
			}

		}
		if isLeft && !match {
			tmpRow := NullRow{TableName: unJoinedTab.TR.Name}
			newTuple := append(tuple, &tmpRow)
			intermresult = append(intermresult, newTuple)
		}
	}
	if isRight {
		// If a right outer join then add the unmatched rows to the intermresult
		for i, match := range rightMatch {
			if !match {
				newTuple := make([]RowInterface, len(joined))
				for j, jtab := range joined {
					//for each table already joined, add null results
					newTuple[j] = &NullRow{TableName: jtab.TR.Name.Clone()}
				}
				newTuple = append(newTuple, &unJoinedTab.Rows[i])
				intermresult = append(intermresult, newTuple)

			}
		}
	}
	return intermresult, nil

}

func swapOuterJoin(currentJoin JoinInfo) JoinInfo {
	newJoin := JoinInfo{}
	switch currentJoin.JoinType {
	case tokens.Left:
		newJoin.JoinType = tokens.Right
		log.Debug("Swapping to Right Outer Join")
	case tokens.Right:
		newJoin.JoinType = tokens.Left
		log.Debug("Swapping to Left Outer Join")
	}
	newJoin.TableA = currentJoin.TableB
	newJoin.TableB = currentJoin.TableA
	newJoin.ONClause = currentJoin.ONClause
	return newJoin
}

//ValidateGroupBySemantics validates a query that it follows the group by rules
func (q *Query) ValidateGroupBySemantics(profile *sqprofile.SQProfile) error {
	var err error

	if q.GroupBy == nil && q.EList.HasAggregateFunc() {
		// make sure they are all aggregate funcs
		for _, exp := range q.EList.GetExprs() {
			if exp.IsAggregate() {
				continue
			}
			fexp, ok := exp.(*FuncExpr)
			if ok {
				return sqerr.NewSyntaxf("%s is not an aggregate function", fexp.Name())
			}
			return sqerr.NewSyntax("Select Statements with Aggregate functions (count, sum, min, max, avg) must not have other expressions")
		}
	}
	// verify all groupby cols exist in table list
	if q.GroupBy != nil {
		if err = q.GroupBy.ValidateCols(profile, q.Tables); err != nil {
			return err
		}

		for _, exp := range q.GroupBy.GetExprs() {
			if q.EList.FindName(exp.Name()) == -1 {
				return sqerr.NewSyntaxf("%s is not in the expression list: %s", exp.Name(), q.EList.String())
			}
		}
		for _, exp := range q.EList.exprlist {
			if exp.IsAggregate() {
				continue
			}
			fexp, ok := exp.(*FuncExpr)
			if ok {
				return sqerr.NewSyntaxf("%s is not an aggregate function", fexp.Name())
			}
			if q.GroupBy.FindName(exp.Name()) == -1 {
				return sqerr.NewSyntaxf("%s is not in the group by clause: %v", exp.Name(), q.GroupBy.String())
			}
		}
	}
	if q.HavingExpr != nil {
		h := *q.HavingExpr
		err = h.ValidateCols(profile, q.Tables)
		if err != nil {
			return err
		}
		newHaving, flist, cnt := ProcessHaving(h, nil, q.EList.Len())
		q.HavingExpr = &newHaving
		for _, f := range flist {
			q.EList.AddHidden(&f, true)
		}
		if cnt != q.EList.Len() {
			return sqerr.NewInternalf("eList len: %d != cnt: %d", q.EList.Len(), cnt)
		}
	}
	return nil
}

// ProcessGroupBy sorts and removes duplicate rows in the data set
func (q *Query) ProcessGroupBy(profile *sqprofile.SQProfile, d *DataSet) error {
	var err error
	var gbOrder []OrderItem

	funcEx, funcIdx := q.EList.FindAggregateFuncs()
	colCnt := make([]int, q.EList.Len())
	//sort by the group by Cols

	if q.GroupBy != nil {
		//save the original order
		oldOrder := d.order
		defer func() {
			d.order = oldOrder
		}()
		d.order = nil

		// Set the sort order to be the same as the group by & Sort
		gbOrder = make([]OrderItem, q.GroupBy.Len())
		for i, expr := range q.GroupBy.GetExprs() {
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
			if q.EList.Len() != len(d.Vals[i]) {
				return sqerr.NewInternalf("Expression list len (%d) does not match value list len (%d)", q.EList.Len(), len(d.Vals[i]))
			}
			res, colCnt = initResultRow(q.EList.Len(), d.Vals[i])
			result = append(result, res)
			grpCnt = 1

		} else {
			grpCnt++
			result[resultIdx], colCnt, err = calcAggregates(d.Vals[i], result[resultIdx], funcEx, funcIdx, colCnt)
		}
		match = q.GroupBy == nil

		if q.GroupBy != nil && i < len(d.Vals)-1 {
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

	err = q.filterHaving(profile, d)
	return err
}

func (q *Query) filterHaving(profile *sqprofile.SQProfile, d *DataSet) error {
	// If there is not an expr then do nothing
	if q.HavingExpr == nil {
		return nil
	}
	btrue := sqtypes.NewSQBool(true)
	//Figure out how many hidden cols
	mark := -1
	for x := q.EList.Len() - 1; x >= 0; x-- {
		if !q.EList.isHidden[x] {
			break
		}
		mark = x
	}

	var result [][]sqtypes.Value
	h := *q.HavingExpr
	for i, r := range d.Vals {

		row := DSRow{Ptr: sqptr.SQPtr(i), Vals: r, TableName: ""}
		res, err := h.Evaluate(profile, false, &row)
		if err != nil {
			return err
		}

		if res.Equal(btrue) {
			result = append(result, row.Vals[:mark])
		}
	}
	q.EList.exprlist = q.EList.exprlist[:mark]
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
