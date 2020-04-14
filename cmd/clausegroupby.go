package cmd

import (
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

//GroupByClause processing
func GroupByClause(tkns *tokens.TokenList) (*sqtables.ExprList, error) {

	if tkns.Test(tokens.Group) != nil {
		tkns.Remove()
	}
	if tkns.Test(tokens.By) == nil {
		return nil, sqerr.NewSyntax("GROUP missing BY")
	}
	tkns.Remove()

	eList, err := GetExprList(tkns, tokens.NilToken, tokens.Group)
	if err != nil {
		return nil, err
	}

	if eList.HasAggregateFunc() {
		flist, _ := eList.FindAggregateFuncs()
		expr := flist[0]
		return nil, sqerr.NewSyntaxf("GROUP BY clause expression can't contain aggregate functions: %s", expr.Name())
	}
	return eList, nil

}
