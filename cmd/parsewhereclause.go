package cmd

import (
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

// ParseWhereClause takes a token list and extracts the where clause
func ParseWhereClause(tkns *tokens.TokenList, terminators ...tokens.TokenID) (whereExpr sqtables.Expr, err error) {

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

	whereExpr, err = whereExpr.Reduce()
	if err != nil {
		return nil, err
	}

	return
}
