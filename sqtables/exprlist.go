package sqtables

import (
	"strings"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtypes"
)

//ExprList a list of Expresions
type ExprList struct {
	exprlist []Expr
	isCount  bool
}

//Len the number of expressions in list
func (el *ExprList) Len() int {
	return len(el.exprlist)
}

//Evaluate evaluates all of the expressions given a row
func (el *ExprList) Evaluate(profile *sqprofile.SQProfile, row *RowDef) ([]sqtypes.Value, error) {
	var err error
	vals := make([]sqtypes.Value, len(el.exprlist))
	for i, e := range el.exprlist {
		vals[i], err = e.Evaluate(profile, row)
		if err != nil {
			return nil, err
		}
	}
	return vals, nil
}

// ToString returns a string representation of the expression list
func (el *ExprList) ToString() string {
	str := ""
	for _, e := range el.exprlist {
		str += e.ToString() + ","
	}
	if len(str) == 0 {
		return ""
	}
	return str[:len(str)-1]
}

//Add an expression to the end of the list
func (el *ExprList) Add(e Expr) {
	_, ok := e.(*CountExpr)
	if ok {
		el.isCount = true
	}
	el.exprlist = append(el.exprlist, e)
}

//Pop removes an expression from the end of the list
func (el *ExprList) Pop() Expr {
	n := el.Len() - 1
	if n < 0 {
		return nil
	}
	ex := el.exprlist[n]
	el.exprlist = el.exprlist[:n]
	return ex
}

// ColsToExpr creates an expression list out of a col list
func ColsToExpr(clist ColList) *ExprList {
	var elist ExprList
	cols := clist.GetColDefs()
	for _, col := range cols {
		elist.Add(NewColExpr(col))
	}

	return &elist
}

// GetNames produces a string array of the names of each item in the list
func (el *ExprList) GetNames() []string {
	names := make([]string, el.Len())
	for i, ex := range el.exprlist {
		names[i] = ex.Name()
	}
	return names
}

// FindName gets the index of the named expression in the list
func (el *ExprList) FindName(name string) int {
	idx := -1
	name = strings.ToLower(name)
	for i, ex := range el.exprlist {
		if strings.ToLower(ex.Name()) == name {
			idx = i
			break
		}
	}
	return idx
}

// ValidateCols takes a column list and checks all potential columns against that list
func (el *ExprList) ValidateCols(profile *sqprofile.SQProfile, tab *TableDef) error {
	for _, ex := range el.exprlist {
		// Check for a ColExpr
		err := ex.ValidateCols(profile, tab)
		if err != nil {
			return err
		}
	}
	return nil
}

//NewExprList creates an ExprList from a series of expressions
func NewExprList(exprs ...Expr) *ExprList {
	var el ExprList
	for _, ex := range exprs {
		el.Add(ex)
	}
	return &el
}

// HasCount indicates if the list has a count function expression
func (el *ExprList) HasCount() bool {
	return el.isCount
}

//GetValues returns a list of values if all expressions reduce to a value
func (el *ExprList) GetValues() ([]sqtypes.Value, error) {
	var err error
	var expr Expr

	vals := make([]sqtypes.Value, len(el.exprlist))
	for i, e := range el.exprlist {
		expr, err = e.Reduce()
		if err != nil {
			return nil, err
		}
		v, ok := expr.(*ValueExpr)
		if !ok {
			return nil, sqerr.NewSyntax("Expression did not reduce to a Value")
		}
		vals[i] = v.v
	}
	return vals, nil
}

// Encode uses sqbin.Codec to return a binary encoded version of the list
func (el *ExprList) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	enc.WriteInt(el.Len())
	for _, ex := range el.exprlist {
		buff := ex.Encode()
		enc.Write(buff.Bytes())
	}
	return enc
}

// DecodeExprList uses sqbin.Codec to return a list from a binary encoded version of the list
func DecodeExprList(dec *sqbin.Codec) *ExprList {
	eList := ExprList{}
	l := dec.ReadInt()
	for i := 0; i < l; i++ {
		eList.Add(DecodeExpr(dec))
	}
	return &eList
}

// NewExprListFromValues creates a new expression list from an array of values
func NewExprListFromValues(vals []sqtypes.Value) *ExprList {
	var eList ExprList
	for _, val := range vals {
		eList.Add(NewValueExpr(val))
	}
	return &eList
}
