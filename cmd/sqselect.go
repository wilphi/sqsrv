package cmd

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	e "github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	t "github.com/wilphi/sqsrv/tokens"
)

// Select -
func Select(profile *sqprofile.SQProfile, tkns *t.TokenList) (string, *sqtables.DataSet, error) {
	data, err := SelectFromTokens(profile, tkns)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("%d rows found", data.NumRows()), data, err
}

// SelectFromTokens - takes a list of tokens returns:
//	 DataSet - All data found by select statement
//   error - if !nil an error has occurred
func SelectFromTokens(profile *sqprofile.SQProfile, tkns *t.TokenList) (*sqtables.DataSet, error) {
	var err error
	var colNames []string
	var cols sqtables.ColList
	var isAsterix = false
	var tableName string
	var td *sqtables.TableDef
	var whereConditions sqtables.Condition
	log.Info("SELECT statement...")

	//eat SELECT token
	tkns.Remove()

	// Get the column list. * is special case
	if tkns.Test(t.Asterix) != "" {
		tkns.Remove()
		isAsterix = true

		// eat the From
		if tkns.Test(t.From) == "" {
			// no FROM
			return nil, e.NewSyntax("Expecting FROM")
		}
		tkns.Remove()
	} else {
		// get the column list
		tkns, colNames, err = GetIdentList(tkns, t.AllWordTokens[t.From], true)
		if err != nil {
			return nil, err
		}
	}

	//expecting Ident (tablename)
	if tableName = tkns.Test(t.Ident); tableName == "" {
		return nil, e.NewSyntax("Expecting table name in select statement")
	}
	tkns.Remove()

	// get the TableDef
	td = sqtables.GetTable(profile, tableName)
	if td == nil {
		return nil, e.New("Table " + tableName + " does not exist for select statement")
	}

	// Once we have the table name we can generate the column list
	if isAsterix {
		cols = td.GetCols(profile)
	} else {
		//convert into column defs
		cols = sqtables.NewColListNames(colNames)
		err = cols.ValidateTable(profile, td)
		if err != nil {
			return nil, err
		}
	}

	// Optional Where clause processing goes here
	if tkns.Test(t.Where) != "" {
		tkns.Remove()
		tkns, whereConditions, err = GetWhereConditions(profile, tkns, td)
		if err != nil {
			return nil, err
		}
	}

	return SelectFromTable(profile, tableName, cols, whereConditions)
}

// SelectFromTable -
func SelectFromTable(profile *sqprofile.SQProfile, tableName string, cols sqtables.ColList, whereConditions sqtables.Condition) (*sqtables.DataSet, error) {
	t := sqtables.GetTable(profile, tableName)
	if t == nil {
		return nil, e.New("Table " + tableName + " does not exist for select statement")
	}
	data, err := t.GetRowData(profile, cols, whereConditions)
	if err != nil {
		return nil, err
	}

	return data, nil
}
