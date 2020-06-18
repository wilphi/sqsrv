package cmd

import (
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/tokens"
)

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// GetTableList - get a comma separated list of Tables ended by a terminatorID token.
func GetTableList(profile *sqprofile.SQProfile, tkns *tokens.TokenList, terminators ...tokens.TokenID) (*sqtables.TableList, error) {
	var err error
	isHangingComma := false
	tables := sqtables.NewTableList(profile, nil)
	// loop to get the table names
	for {
		if tkns.IsEmpty() || tkns.TestTkn(terminators...) != nil {
			if isHangingComma {
				return nil, sqerr.NewSyntax("Unexpected ',' in From clause")
			}
			break
		}
		if tables.Len() > 0 && !isHangingComma {
			return nil, sqerr.NewSyntax("Comma is required to separate tables")
		}
		// Ident(tableName),  opt comma
		if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
			tName := tkn.(*tokens.ValueToken).Value()
			tkns.Remove()

			// Check for an Alias
			if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
				aName := tkn.(*tokens.ValueToken).Value()
				err = tables.Add(profile, sqtables.TableRef{Name: moniker.New(tName, aName)})
				tkns.Remove()
			} else {
				err = tables.Add(profile, sqtables.TableRef{Name: moniker.New(tName, "")})
			}
			if err != nil {
				return nil, err
			}
			// check for optional comma
			if tkns.IsA(tokens.Comma) {
				isHangingComma = true
				tkns.Remove()
			} else {
				isHangingComma = false
			}
		} else {
			return nil, sqerr.NewSyntax("Expecting name of Table")

		}
	}
	if tables.Len() <= 0 {
		return nil, sqerr.NewSyntax("No Tables defined for query")
	}

	return tables, nil
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// ParseFromClause - parses the tables and the join conditions of a From Clause
func ParseFromClause(profile *sqprofile.SQProfile, tkns *tokens.TokenList, terminators ...tokens.TokenID) (*sqtables.TableList, []sqtables.JoinInfo, error) {
	var err error
	var joinExprs []sqtables.JoinInfo
	var join *sqtables.JoinInfo

	//	commaTerms := append(terminators, tokens.Comma)

	tables := sqtables.NewTableList(profile, nil)
	// loop to get the table names
	//	for {
	if tkns.IsEmpty() || tkns.TestTkn(terminators...) != nil {
		return nil, nil, sqerr.NewSyntax("No Tables defined for query")
	}
	// Ident(tableName)
	if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
		lastTName := parseMoniker(tkns)
		err = tables.Add(profile, sqtables.TableRef{Name: lastTName.Clone()})
		if err != nil {
			return nil, nil, err
		}

		// check for join
		for {
			lastTName, join, err = parseJoin(profile, tkns, tables, lastTName, terminators...)
			if err != nil {
				return nil, nil, err
			}
			if join == nil {
				//No join so exit loop
				break
			}
			joinExprs = append(joinExprs, *join)
		}

		if tkns.IsA(tokens.Outer) {
			return nil, nil, sqerr.NewSyntax("LEFT or RIGHT keyword required before OUTER JOIN")
		}
		// check for optional comma
		if tkns.IsA(tokens.Comma) {
			return nil, nil, sqerr.NewSyntax("Unexpected ',' in From clause")
		}
		if !tkns.IsEmpty() && tkns.TestTkn(terminators...) == nil {
			switch v := tkns.Peek().(type) {
			case *tokens.WordToken:
				return nil, nil, sqerr.NewSyntaxf("Unexpected end of From clause at %s", v.Name())
			case *tokens.ValueToken:
				return nil, nil, sqerr.NewSyntaxf("Missing Join in From clause near %q", v.Value())
			}

		}
	} else {
		return nil, nil, sqerr.NewSyntax("Expecting name of Table")

	}

	if tables.Len() <= 0 {
		return nil, nil, sqerr.NewSyntax("No Tables defined for query")
	}

	return tables, joinExprs, nil
}

func parseMoniker(tkns *tokens.TokenList) *moniker.Moniker {
	var name, alias string
	// Ident(tableName),  opt comma
	if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
		name = tkn.(*tokens.ValueToken).Value()
		tkns.Remove()

		// Check for an Alias
		if tkn := tkns.TestTkn(tokens.Ident); tkn != nil {
			alias = tkn.(*tokens.ValueToken).Value()
			tkns.Remove()
		}
	}
	if name == "" {
		// no tablename found
		return nil
	}
	return moniker.New(name, alias)
}

func parseJoin(profile *sqprofile.SQProfile, tkns *tokens.TokenList, tables *sqtables.TableList, lastTName *moniker.Moniker,
	terminators ...tokens.TokenID) (*moniker.Moniker, *sqtables.JoinInfo, error) {

	var joinType tokens.TokenID
	var joinExpr sqtables.Expr
	var tkn tokens.Token
	var joinName string
	var err error
	var join sqtables.JoinInfo
	var TableA, TableB sqtables.TableRef

	if tkn = tkns.TestTkn(tokens.Inner, tokens.Left, tokens.Right, tokens.Full, tokens.Join, tokens.Cross); tkn == nil {
		return nil, nil, nil
	}
	joinType = tkn.ID()
	joinName = tokens.IDName(joinType)

	tkns.Remove()

	if joinType == tokens.Left || joinType == tokens.Right || joinType == tokens.Full {
		if !tkns.IsA(tokens.Outer) {
			return nil, nil, sqerr.NewSyntaxf("Expecting OUTER keyword after join type %s", joinName)
		}
		tkns.Remove()
		joinName += " " + tokens.IDName(tokens.Join)
	}

	if joinType != tokens.Join {
		if !tkns.IsA(tokens.Join) {
			return nil, nil, sqerr.NewSyntaxf("Expecting JOIN keyword after %s", joinName)
		}
		tkns.Remove()
	} else {
		joinType = tokens.Inner
	}

	tname := parseMoniker(tkns)
	if tname == nil {
		return nil, nil, sqerr.NewSyntax("Expecting a tablename after JOIN")
	}
	TableB = sqtables.TableRef{Name: tname.Clone()}

	err = tables.Add(profile, TableB)
	if err != nil {
		return nil, nil, err
	}

	if joinType != tokens.Cross {
		if !tkns.IsA(tokens.On) {
			return nil, nil, sqerr.NewSyntax("Expecting keyword ON after second table name in a JOIN")
		}
		tkns.Remove()

		joinExpr, err = ParseWhereClause(tkns, true, terminators...)
		if err != nil {
			return nil, nil, err
		}

		err = joinExpr.ValidateCols(profile, tables)
		if err != nil {
			return nil, nil, err
		}

		// make sure that the table has a column that is used in the ON statement
		var col *column.Ref
		cols := joinExpr.ColRefs()
		for _, cr := range cols {
			if moniker.Equal(cr.TableName, tname) {
				col = &cr
			} else {
				TableA = sqtables.TableRef{Name: cr.TableName.Clone()}
			}
		}

		if col == nil {
			return nil, nil, sqerr.NewSyntaxf("The table %s must be used as a join condition in the ON statement", tname)
		}

		join.JoinType = joinType
		join.ONClause = joinExpr
		join.TableA = TableA
		join.TableB = TableB

		err = validateOnClause(joinExpr)
	} else {
		// Cross Join
		if tkns.IsA(tokens.On) {
			return nil, nil, sqerr.NewSyntax("Cross joins must not have an ON expression")
		}
		join.JoinType = joinType
		join.ONClause = joinExpr
		join.TableA = sqtables.TableRef{Name: lastTName.Clone()}
		join.TableB = TableB

	}

	return TableB.Name, &join, err
}

func validateOnClause(OnClause sqtables.Expr) error {

	// OpExpr(colExpr, ColExpr)
	opx, ok := OnClause.(*sqtables.OpExpr)
	if !ok {
		return sqerr.NewSyntax("ON clause must take the form of <tablea>.<colx> = <tableb>.<coly>")
	}
	if opx.Operator == tokens.And || opx.Operator == tokens.Or {
		return sqerr.New("Multi column joins are not currently implemented")
	}

	colL, lok := opx.Left().(*sqtables.ColExpr)
	if !lok {
		return sqerr.NewSyntaxf("Expression to the left of %s in ON clause is not a column", tokens.IDName(opx.Operator))
	}
	colR, rok := opx.Right().(*sqtables.ColExpr)
	if !rok {
		return sqerr.NewSyntaxf("Expression to the right of %s in ON clause is not a column", tokens.IDName(opx.Operator))
	}

	cl := colL.ColRef()
	cr := colR.ColRef()

	if moniker.Equal(cl.TableName, cr.TableName) {
		return sqerr.NewSyntax("To join tables, the ON clause must reference at least two different ones")
	}

	return nil
}
