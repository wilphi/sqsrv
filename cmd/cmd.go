package cmd

import (
	"fmt"
	"strings"

	"github.com/wilphi/sqsrv/sqerr"
	e "github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
	t "github.com/wilphi/sqsrv/tokens"
)

// contains common functions for all commands

// GetIdentList - get a comma separated list of Ident ended by a terminator token.
func GetIdentList(tkns *t.TokenList, terminator string) ([]string, error) {
	var ids []string
	isHangingComma := false
	// loop to get the columns of the INSERT
	for {
		if tkns.Test(terminator) != "" {
			if isHangingComma {
				return nil, e.NewSyntax(fmt.Sprintf("Unexpected %q before %q", ",", t.GetSymbolFromTokenID(terminator)))
			}
			break
		}
		if len(ids) > 0 && !isHangingComma {
			return nil, e.NewSyntax("Comma is required to separate columns")
		}
		// Ident(colName),  opt comma
		if cName := tkns.Test(t.Ident); cName != "" {
			ids = append(ids, cName)
			tkns.Remove()

			// check for optional comma
			if tkns.Test(t.Comma) != "" {
				isHangingComma = true
				tkns.Remove()
			} else {
				isHangingComma = false
			}
		} else {
			return nil, e.NewSyntax("Expecting name of column")

		}
	}
	if len(ids) <= 0 {
		return nil, e.NewSyntax("No columns defined for table")
	}

	// eat terminator
	tkns.Remove()

	return ids, nil
}

func getCompareValCond(profile *sqprofile.SQProfile, tkns *t.TokenList, td *sqtables.TableDef) (sqtables.Condition, error) {
	var colName, sym string
	var err error
	var v sqtypes.Value
	var col sqtables.ColDef

	if val := tkns.Test(t.Ident); val != "" {
		colName = val
		col = *td.FindColDef(profile, colName)
		tkns.Remove()
	} else {
		err = e.NewSyntax("Expecting a column name in where clause")
		return nil, err
	}
	if val := tkns.Test(t.Equal, t.LessThan, t.GreaterThan); val != "" {
		sym = val
		tkns.Remove()
	} else {
		err = e.NewSyntax(fmt.Sprintf("Expecting an operator after column name (%s) in where clause", colName))
		return nil, err
	}
	if tkns.Test(t.Num, t.Quote, t.RWTrue, t.RWFalse) != "" {
		v, err = sqtypes.CreateValueFromToken(*tkns.Peek())
		if err != nil {
			return nil, err
		}
		tkns.Remove()
	} else {
		err = e.NewSyntax(fmt.Sprintf("Expecting a value in where clause after %s %s", colName, sym))
		return nil, err
	}

	return sqtables.NewCVCond(col, sym, v), nil
}

// GetWhereConditions -
func GetWhereConditions(profile *sqprofile.SQProfile, tkns *t.TokenList, td *sqtables.TableDef) (sqtables.Condition, error) {
	var cond, lCond, rCond sqtables.Condition
	var err error

	cond, err = getCompareValCond(profile, tkns, td)
	if err != nil {
		return nil, err
	}
	for {
		if tkns.Len() <= 0 || tkns.Test(t.Order) != "" {
			break
		}
		if val := tkns.Test(t.And, t.Or); val != "" {
			tkns.Remove()
			lCond = cond
			if val == t.And {
				rCond, err = getCompareValCond(profile, tkns, td)
				if err != nil {
					return nil, err
				}
				cond = sqtables.NewANDCondition(lCond, rCond)
			} else {
				rCond, err = GetWhereConditions(profile, tkns, td)
				cond = sqtables.NewORCondition(lCond, rCond)
				break
			}
		} else {
			break
		}
	}

	return cond, nil
}

//OrderByClause processing
func OrderByClause(tkns *t.TokenList) ([]sqtables.OrderItem, error) {
	var sortCol, sortType string
	var orderBy []sqtables.OrderItem

	if tkns.Test(tokens.Order) != "" {
		tkns.Remove()
	}
	if tkns.Test(tokens.By) == "" {
		return nil, e.NewSyntax("ORDER missing BY")
	}
	tkns.Remove()
	hangingComma := true
	for {

		// colName ASC/DESC, ...
		if sortCol = tkns.Test(tokens.Ident); sortCol != "" {
			if !hangingComma {
				return nil, e.NewSyntax("Missing comma in ORDER BY clause")
			}
			tkns.Remove()
			hangingComma = false
			if sortType = tkns.Test(tokens.Asc, tokens.Desc); sortType != "" {
				tkns.Remove()
			} else {
				sortType = tokens.Asc
			}
			orderBy = append(orderBy, sqtables.OrderItem{ColName: sortCol, SortType: sortType})
			if tkns.Test(tokens.Comma) != "" {
				tkns.Remove()
				hangingComma = true
				continue
			}
		} else {
			return nil, e.NewSyntax("Missing column name in ORDER BY clause")
		}

		if tkns.Len() == 0 || tkns.Peek().GetName() != tokens.Ident {
			break
		}
	}

	return orderBy, nil
}

func getValCol(tkns *t.TokenList) (sqtables.Expr, error) {
	var exp sqtables.Expr
	var mSign bool
	exp = nil

	if tkns.Test(t.Minus) != "" {
		mSign = true
		tkns.Remove()
	}
	tkn := tkns.Peek()
	if tkn != nil {
		v, err := sqtypes.CreateValueFromToken(*tkn)
		if err == nil {
			//Token is a value
			exp = sqtables.NewValueExpr(v)
			tkns.Remove()
			if mSign {
				exp = sqtables.NewNegateExpr(exp)

			}
			return exp, nil
		}
		// is token a ColName
		if cName := tkns.Test(t.Ident); cName != "" {
			exp = sqtables.NewColExpr(sqtables.ColDef{ColName: cName})
			tkns.Remove()
			if mSign {
				exp = sqtables.NewNegateExpr(exp)
			}
			return exp, nil
		}
		// Is it a function
		if fName := tkns.Test(t.Count); fName != "" {
			tkns.Remove()
			if !(tkns.Peek().GetName() == t.OpenBracket && tkns.Peekx(1).GetName() == t.CloseBracket) {
				return nil, e.NewSyntax("Count must be followed by ()")
			}
			tkns.Remove()
			tkns.Remove()
			exp = sqtables.NewCountExpr()
			return exp, nil

		}
	}
	return nil, sqerr.NewSyntax("Invalid expression: Unable to find a value or column")
}

var precedence = map[string]int{
	"+": 1,
	"-": 1,
	"*": 2,
	"/": 2,
	"%": 2,
}

// getExpr uses a Operator-precedence parser algorthim based on pseudo code from Wikipedia
//    (see https://en.wikipedia.org/wiki/Operator-precedence_parser for more details)
func getExpr(tkns *t.TokenList, lExp sqtables.Expr, minPrecedence int, terminators ...string) (sqtables.Expr, error) {
	var rExp sqtables.Expr
	var err error

	// Is token the terminator or a comma
	if tkns.Test(terminators...) != "" || tkns.IsEmpty() {
		return nil, nil
	}

	if lExp == nil {
		// Is token a Value or a Col
		lExp, err = getValCol(tkns)
		if err != nil {
			return nil, err
		}
	}

	if tkns.IsEmpty() {
		return lExp, nil
	}
	lookahead := tkns.Peek().GetValue()
	_, ok := precedence[lookahead]
	for precedence[lookahead] >= minPrecedence && ok {
		op := lookahead
		tkns.Remove()
		rExp, err = getValCol(tkns)
		if err != nil {
			return nil, err
		}
		if !tkns.IsEmpty() {
			lookahead = tkns.Peek().GetValue()
			_, ok = precedence[lookahead]
			for precedence[lookahead] > precedence[op] && ok {
				rExp, err = getExpr(tkns, rExp, precedence[lookahead], terminators...)
				if err != nil {
					return nil, err
				}
				if tkns.IsEmpty() {
					return nil, sqerr.NewSyntax("Incomplete expression")
				}

				lookahead = tkns.Peek().GetValue()
			}
		}
		lExp = sqtables.NewOpExpr(lExp, op, rExp)
		if tkns.IsEmpty() {
			break
		}
	}

	//	}

	return lExp, err
}
func ifte(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

// GetExprList - get a comma separated list of Expressions ended by a terminator token.
func GetExprList(tkns *t.TokenList, terminator string, valuesOnly bool) (*sqtables.ExprList, error) {
	var eList sqtables.ExprList
	var hasCount bool

	// loop to get the expressions
	for {
		// get expression
		exp, err := getExpr(tkns, nil, 1, terminator, t.Comma)
		if err != nil {
			return nil, err
		}
		if exp == nil {
			break

		}
		exp2, err := exp.Reduce()
		if err != nil {
			return nil, err
		}
		if strings.Contains(exp2.GetName(), "count()") {
			hasCount = true
		}
		if valuesOnly {
			// Make sure it is a value
			_, ok := exp2.(*sqtables.ValueExpr)
			if !ok {
				return nil, sqerr.NewSyntax(fmt.Sprintf("Expression %q did not reduce to a value", exp2.GetName()))
			}
		}
		eList.Add(exp2)
		// Is token the terminator
		if tkns.Test(terminator) != "" {

			break
		}
		if tkns.Test(t.Comma) != "" {
			tkns.Remove()
			if tkns.Test(terminator) != "" {

				return nil, sqerr.NewSyntax(fmt.Sprintf("Unexpected %q before %q", ",", t.GetSymbolFromTokenID(terminator)))
			}
		} else {
			return nil, sqerr.NewSyntax("Comma is required to separate " + ifte(valuesOnly, "values", "columns"))
		}
	}
	if eList.Len() <= 0 {
		return nil, e.NewSyntax(ifte(valuesOnly, "No values defined", "No columns defined for query"))
	}
	if hasCount && eList.Len() > 1 {
		return nil, e.NewSyntax("Select Statements with Count() must not have other expressions")
	}

	if tkns.Test(terminator) == "" {
		return nil, sqerr.NewSyntax("Expecting " + ifte(valuesOnly, "value", "name of column") + " or a valid expression")

	}
	// eat terminator
	tkns.Remove()

	return &eList, nil
}
