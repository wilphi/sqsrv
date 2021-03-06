package cmd

import (
	"strings"

	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

// GetIdentList - get a comma separated list of Ident ended by a terminatorID token.
func GetIdentList(tkns *tokens.TokenList, terminatorID tokens.TokenID) ([]string, error) {
	var ids []string
	isHangingComma := false
	// loop to get the columns of the INSERT
	for {
		if (terminatorID == tokens.NilToken && tkns.IsEmpty()) || (terminatorID != tokens.NilToken && tkns.Peek() == tokens.GetWordToken(terminatorID)) {
			if isHangingComma {
				msg := ""
				if tkns.IsEmpty() {
					msg = "the end of statement"
				} else {
					msg = tkns.Peek().Name()
				}
				return nil, sqerr.NewSyntaxf("Unexpected %q before %q", ",", msg)
			}
			break
		}
		if len(ids) > 0 && !isHangingComma {
			return nil, sqerr.NewSyntax("Comma is required to separate columns")
		}
		// Ident(colName),  opt comma
		if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
			cName := tkn.(*tokens.ValueToken).Value()
			ids = append(ids, cName)
			tkns.Remove()

			// check for optional comma
			if tkns.IsA(tokens.Comma) {
				isHangingComma = true
				tkns.Remove()
			} else {
				isHangingComma = false
			}
		} else {
			return nil, sqerr.NewSyntax("Expecting name of column")

		}
	}
	if len(ids) <= 0 {
		return nil, sqerr.NewSyntax("No columns defined for table")
	}

	// eat terminatorID
	tkns.Remove()

	return ids, nil
}

//OrderByClause processing
func OrderByClause(tkns *tokens.TokenList) ([]sqtables.OrderItem, error) {

	if tkns.IsA(tokens.Order) {
		tkns.Remove()
	}
	if !tkns.IsARemove(tokens.By) {
		return nil, sqerr.NewSyntax("ORDER missing BY")
	}

	return ParseColSortOrder(tkns)
}

// ParseColSortOrder processes a column list with sort order info (ASC, DESC) for each col
func ParseColSortOrder(tkns *tokens.TokenList) ([]sqtables.OrderItem, error) {
	var sortCol string
	var sortType tokens.TokenID
	var orderBy []sqtables.OrderItem
	hangingComma := true
	for {

		// colName ASC/DESC, ...
		if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
			sortCol = tkn.(*tokens.ValueToken).Value()
			if !hangingComma {
				return nil, sqerr.NewSyntax("Missing comma in ORDER BY clause")
			}
			tkns.Remove()
			if tkns.IsA(tokens.Period) {
				tkns.Remove()
				if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
					sortCol += "." + tkn.(*tokens.ValueToken).Value()
				} else {
					//Must be an Ident after TableName.
					return nil, sqerr.NewSyntaxf("Column name must follow %s.", sortCol)
				}
				tkns.Remove()
			}
			hangingComma = false
			if tkn := tkns.TestTkn(tokens.Asc, tokens.Desc); tkn != nil {
				sortType = tkns.Peek().ID()
				tkns.Remove()
			} else {
				sortType = tokens.Asc
			}
			orderBy = append(orderBy, sqtables.OrderItem{ColName: sortCol, SortType: sortType})
			if tkns.IsA(tokens.Comma) {
				tkns.Remove()
				hangingComma = true
				continue
			}
		} else {
			return nil, sqerr.NewSyntax("Missing column name in ORDER BY clause")
		}

		if tkns.Len() == 0 || tkns.Peek().ID() != tokens.Ident {
			break
		}
	}

	return orderBy, nil
}

func getValCol(tkns *tokens.TokenList) (exp sqtables.Expr, err error) {
	var mSign bool
	var v sqtypes.Value
	var tName string

	if tkns.IsA(tokens.Minus) {
		mSign = true
		tkns.Remove()
	}
	if tkns.IsA(tokens.OpenBracket) {
		tkns.Remove()
		exp, err = GetExpr(tkns, nil, 0, tokens.CloseBracket)
		if !tkns.IsA(tokens.CloseBracket) {
			return nil, sqerr.NewSyntax("'(' does not have a matching ')'")
		}
		tkns.Remove()
		return exp, err
	}

	if tkn := tkns.Peek(); tkn != nil {
		v, err = sqtypes.CreateValueFromToken(tkn)
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
		if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
			cName := tkn.(*tokens.ValueToken).Value()
			tName = ""
			displayTable := false
			tkns.Remove()
			if tkns.IsA(tokens.Period) {
				tkns.Remove()
				tName = cName
				if tkn = tkns.TestTkn(tokens.Ident); tkn == nil {
					//Not the expected table name
					return nil, sqerr.NewSyntaxf("Expecting column after %s.", tName)
				}
				cName = tkn.(*tokens.ValueToken).Value()
				tkns.Remove()
				displayTable = true
			}
			// moniker.New(tName,tName)
			exp = sqtables.NewColExpr(column.Ref{ColName: cName, TableName: moniker.New(tName, ""), DisplayTableName: displayTable})
			if mSign {
				exp = sqtables.NewNegateExpr(exp)
			}
			return exp, nil
		}
		// Is it a function
		if ftkn := tkns.Peek(); ftkn.TestFlags(tokens.IsFunction) {
			tkns.Remove()
			cmd := ftkn.ID()
			// Must be followed by open bracket
			if tkns.IsEmpty() || tkns.Peek().ID() != tokens.OpenBracket {
				return nil, sqerr.NewSyntaxf("Function %s must be followed by (", tokens.IDName(cmd))
			}
			tkns.Remove()
			if tkns.IsEmpty() {
				if ftkn.TestFlags(tokens.IsNoArg) {
					return nil, sqerr.NewSyntaxf("%s must be followed by ()", tokens.IDName(cmd))
				}
				return nil, sqerr.NewSyntaxf("Function %s is missing an expression followed by )", tokens.IDName(cmd))
			}
			// No Args if close bracket
			if tkns.Peek().ID() == tokens.CloseBracket {
				tkns.Remove()
				if !ftkn.TestFlags(tokens.IsNoArg) {
					return nil, sqerr.NewSyntaxf("Function %s is missing an expression between ( and )", tokens.IDName(cmd))
				}
				exp = sqtables.NewFuncExpr(cmd, nil)
				return exp, nil
			}
			// At least one arg
			exp, err = GetExpr(tkns, nil, 0, tokens.CloseBracket)
			if err != nil {
				if strings.Contains(err.Error(), "Unable to find a value or column near FROM") {
					err = sqerr.NewSyntaxf("No arguments or ) for function %s", tokens.IDName(cmd))
				}
				return nil, err
			}
			if exp == nil {
				return nil, sqerr.NewSyntaxf("Function %s is missing an expression between ( and )", ftkn.Name())
			}
			if tkns.IsEmpty() || tkns.Peek().ID() != tokens.CloseBracket {
				return nil, sqerr.NewSyntaxf("Function %s is missing ) after expression", ftkn.Name())
			}
			tkns.Remove()
			fexp := sqtables.NewFuncExpr(tkn.ID(), exp)
			return fexp, nil

		}
	}
	if tkns.IsEmpty() {
		return nil, sqerr.NewSyntax("Unexpected end to expression")
	}
	vtkn, ok := tkns.Peek().(*tokens.ValueToken)
	val := ""
	if ok {
		val = vtkn.Value()
	} else {
		val = tkns.Peek().Name()
	}
	return nil, sqerr.NewSyntaxf("Invalid expression: Unable to find a value or column near %s", val)
}

var exPrecedence = map[tokens.TokenID]int{
	tokens.Or:               0,
	tokens.And:              1,
	tokens.Equal:            2,
	tokens.NotEqual:         2,
	tokens.LessThan:         2,
	tokens.GreaterThan:      2,
	tokens.LessThanEqual:    2,
	tokens.GreaterThanEqual: 2,
	tokens.Plus:             3,
	tokens.Minus:            3,
	tokens.Asterix:          4,
	tokens.Divide:           4,
	tokens.Modulus:          4,
}

// GetExpr uses a Operator-precedence parser algorthim based on pseudo code from Wikipedia
//    (see https://en.wikipedia.org/wiki/Operator-precedence_parser for more details)
func GetExpr(tkns *tokens.TokenList, lExp sqtables.Expr, minPrecedence int, terminators ...tokens.TokenID) (sqtables.Expr, error) {
	var rExp sqtables.Expr
	var err error

	// Is token the terminatorID or a comma
	if tkns.TestTkn(terminators...) != nil || tkns.IsEmpty() {
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
	lookahead := tkns.Peek().ID()
	_, ok := exPrecedence[lookahead]
	for exPrecedence[lookahead] >= minPrecedence && ok {
		op := lookahead
		tkns.Remove()
		rExp, err = getValCol(tkns)
		if err != nil {
			return nil, err
		}
		if !tkns.IsEmpty() {
			lookahead = tkns.Peek().ID()
			_, ok = exPrecedence[lookahead]
			for exPrecedence[lookahead] > exPrecedence[op] && ok {
				rExp, err = GetExpr(tkns, rExp, exPrecedence[lookahead], terminators...)
				if err != nil {
					return nil, err
				}
				if tkns.IsEmpty() {
					break
				}

				lookahead = tkns.Peek().ID()
				_, ok = exPrecedence[lookahead]
			}
		}
		lExp = sqtables.NewOpExpr(lExp, op, rExp)
		if tkns.IsEmpty() {
			break
		}
	}

	return lExp, err
}

func ifte(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

// GetExprList - get a comma separated list of Expressions ended by a terminatorID token.
// listtype is one of:
//      tokens.Values - VALUES clause for INSERT,
//      tokens.Select - expressions for SELECT clause,
//      tokens.Group - expressions for GROUP BY clause
func GetExprList(tkns *tokens.TokenList, terminatorID tokens.TokenID, listtype tokens.TokenID) (*sqtables.ExprList, error) {
	var eList sqtables.ExprList

	// loop to get the expressions
	for {
		// get expression
		exp, err := GetExpr(tkns, nil, 1, terminatorID, tokens.Comma)
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
		if listtype == tokens.Values {
			// Make sure it is a value
			_, ok := exp2.(*sqtables.ValueExpr)
			if !ok {
				return nil, sqerr.NewSyntaxf("Expression %q did not reduce to a value", exp2.Name())
			}
		}

		// Check for optional alias
		if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
			alias := tkn.(*tokens.ValueToken).Value()
			tkns.Remove()
			exp2.SetAlias(alias)
		}
		eList.Add(exp2)
		// Is token the terminatorID
		if tkns.IsA(terminatorID) || terminatorID == tokens.NilToken && tkns.IsEmpty() || tkns.IsReservedWord() {
			break
		}
		if tkns.IsA(tokens.Comma) {
			tkns.Remove()
			if tkns.IsA(terminatorID) {

				return nil, sqerr.NewSyntaxf("Unexpected %q before %q", ",", tokens.IDName(terminatorID))
			}
		} else {
			return nil, sqerr.NewSyntax("Comma is required to separate " + ifte(listtype == tokens.Values, "values", "expressions"))
		}
	}
	if eList.Len() <= 0 {
		errStr := ""
		switch listtype {
		case tokens.Values:
			errStr += "No values defined"
		case tokens.Select:
			errStr += "No expressions defined for SELECT clause"
		case tokens.Group:
			errStr += "No expressions defined for GROUP BY clause"
		default:
			errStr += "No expressions defined for " + tokens.IDName(listtype)
		}
		return nil, sqerr.NewSyntax(errStr)
	}

	if !(terminatorID == tokens.NilToken && tkns.IsEmpty()) && !tkns.IsA(terminatorID) && !tkns.IsReservedWord() {
		errStr := ""
		switch listtype {
		case tokens.Values:
			errStr += "Expecting a value"
		case tokens.Select, tokens.Group:
			errStr += "Expecting name of column or a valid expression"
		default:
			errStr += "Expecting name of column or a valid expression for " + tokens.IDName(listtype)
		}
		return nil, sqerr.NewSyntax(errStr)
	}

	return &eList, nil
}
