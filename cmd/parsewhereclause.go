package cmd

import (
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/tokens"
)

// ParseWhereClause takes a token list and extracts the where clause
func ParseWhereClause(tkns *tokens.TokenList, allowJoin bool, terminators ...tokens.TokenID) (whereExpr sqtables.Expr, err error) {

	whereExpr, err = GetExpr(tkns, nil, 0, terminators...)
	if err != nil {
		return nil, err
	}
	// Make sure that aggregate functions are not used in where clause
	if whereExpr.IsAggregate() {
		flist := sqtables.FindAggregateFuncs(whereExpr)
		str := ""
		if len(flist) > 0 {
			str = "(" + tokens.IDName(flist[0].Cmd) + ") "
		}
		return nil, sqerr.NewSyntaxf("Aggregate functions %sare not allowed in Where clause", str)
	}

	// make sure there are no join type expressions in where clause
	if !allowJoin {
		err = findJoinExpr(whereExpr)
		if err != nil {
			return nil, err
		}
	}

	// Reduce the where clause to minimize computation
	return whereExpr.Reduce()
}

func findJoinExpr(exp sqtables.Expr) error {
	var err error
	/*
		// make sure that there is a valid expression
		if exp == nil {
			return nil
		}
	*/
	// OpExpr(colExpr, ColExpr)
	opx, ok := exp.(*sqtables.OpExpr)
	if !ok {
		return nil
	}
	if opx.Operator == tokens.And || opx.Operator == tokens.Or {
		err = findJoinExpr(opx.Left())
		if err != nil {
			return err
		}
		err = findJoinExpr(opx.Right())
		if err != nil {
			return err
		}

	}

	colL, lok := opx.Left().(*sqtables.ColExpr)
	if !lok {
		return findJoinExpr(opx.Left())
	}
	colR, rok := opx.Right().(*sqtables.ColExpr)
	if !rok {
		return findJoinExpr(opx.Right())
	}

	cl := colL.ColRef()
	cr := colR.ColRef()

	if !moniker.Equal(cl.TableName, cr.TableName) {
		return sqerr.NewSyntax("To join tables use the On condition in the From Clause")
	}

	return nil
}
