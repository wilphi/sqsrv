package cmd

import (
	"fmt"

	e "github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
	t "github.com/wilphi/sqsrv/tokens"
)

// contains common functions for all commands

// GetIdentList - get a comma separated list of Ident ended by a terminator token.
func GetIdentList(tkns *t.TokenList, terminator *t.Token, allowFunctions bool) (*t.TokenList, []string, error) {
	var ids []string
	isHangingComma := false
	// loop to get the columns of the INSERT
	for {
		if tkns.Test(terminator.GetName()) != "" {
			if isHangingComma {
				return tkns, nil, e.NewSyntax("Unexpected \",\" before \"" + terminator.GetValue() + "\"")
			}
			break
		}
		if len(ids) > 0 && !isHangingComma {
			return tkns, nil, e.NewSyntax("Comma is required to separate column definitions")
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
			if cName := tkns.Test(t.Count); allowFunctions && cName != "" {
				// Count ( )
				ids = append(ids, cName)
				tkns.Remove()
				if tkns.Peek().GetName() == t.OpenBracket && tkns.Peekx(1).GetName() == t.CloseBracket {
					tkns.Remove()
					tkns.Remove()
				} else {
					return tkns, nil, e.NewSyntax("Count must be followed by ()")
				}
			} else {
				return tkns, nil, e.NewSyntax("Expecting name of column")
			}
		}
		//tkns.Remove()
	}
	if len(ids) <= 0 {
		return tkns, nil, e.NewSyntax("No columns defined for table")
	}

	// eat terminator
	tkns.Remove()

	return tkns, ids, nil
}

func getCompareValCond(profile *sqprofile.SQProfile, tkns *t.TokenList, td *sqtables.TableDef) (*t.TokenList, sqtables.Condition, error) {
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
		return tkns, nil, err
	}
	if val := tkns.Test(t.Equal, t.LessThan, t.GreaterThan); val != "" {
		sym = val
		tkns.Remove()
	} else {
		err = e.NewSyntax(fmt.Sprintf("Expecting an operator after column name (%s) in where clause", colName))
		return tkns, nil, err
	}
	if tkns.Test(t.Num, t.Quote, t.RWTrue, t.RWFalse) != "" {
		v, err = sqtypes.CreateValueFromToken(*tkns.Peek())
		if err != nil {
			return tkns, nil, err
		}
		tkns.Remove()
	} else {
		err = e.NewSyntax(fmt.Sprintf("Expecting a value in where clause after %s %s", colName, sym))
		return tkns, nil, err
	}

	return tkns, sqtables.NewCVCond(col, sym, v), nil
}

// GetWhereConditions -
func GetWhereConditions(profile *sqprofile.SQProfile, tkns *t.TokenList, td *sqtables.TableDef) (*t.TokenList, sqtables.Condition, error) {
	var cond, lCond, rCond sqtables.Condition
	var err error

	tkns, cond, err = getCompareValCond(profile, tkns, td)
	if err != nil {
		return tkns, nil, err
	}
	for {
		if tkns.Len() <= 0 || tkns.Test(t.Order) != "" {
			break
		}
		if val := tkns.Test(t.And, t.Or); val != "" {
			tkns.Remove()
			lCond = cond
			if val == t.And {
				tkns, rCond, err = getCompareValCond(profile, tkns, td)
				if err != nil {
					return tkns, nil, err
				}
				cond = sqtables.NewANDCondition(lCond, rCond)
			} else {
				tkns, rCond, err = GetWhereConditions(profile, tkns, td)
				cond = sqtables.NewORCondition(lCond, rCond)
				break
			}
		} else {
			break
		}
	}

	return tkns, cond, nil
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
	if hangingComma {
		return nil, e.NewSyntax("Missing comma in ORDER BY clause")
	}
	return orderBy, nil
}
