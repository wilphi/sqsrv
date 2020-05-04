package cmd

import (
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

//HavingClause processing
func HavingClause(tkns *tokens.TokenList, terminators ...tokens.TokenID) (*sqtables.Expr, error) {
	if tkns.IsA(tokens.Having) {
		tkns.Remove()
	}
	havingExpr, err := GetExpr(tkns, nil, 0, terminators...)
	if err != nil {
		return nil, err
	}

	havingExpr, err = havingExpr.Reduce()
	if err != nil {
		return nil, err
	}

	//havingExpr.
	return &havingExpr, nil
}
