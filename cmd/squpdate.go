package cmd

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/redo"
	e "github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtypes"
	t "github.com/wilphi/sqsrv/tokens"
)

// Update implements the SQL command UPDATE
func Update(profile *sqprofile.SQProfile, tkns *t.TokenList) (string, *sqtables.DataSet, error) {
	var err error
	var tableName, colName string
	var setCols []string
	var setVals []sqtypes.Value
	var cond sqtables.Condition

	log.Info("Update statement...")

	// Eat Update Token
	if tkns.Test(t.Update) != "" {
		tkns.Remove()
	}

	//expecting Ident (tablename)
	if tableName = tkns.Test(t.Ident); tableName == "" {
		return "", nil, e.NewSyntax("Expecting table name in Update statement")
	}
	tkns.Remove()
	tab := sqtables.GetTable(profile, tableName)
	if tab == nil {
		return "", nil, e.NewSyntax(fmt.Sprintf("Invalid table name: %s does not exist", tableName))
	}

	// eat the SET
	if tkns.Test(t.Set) == "" {
		// no SET
		return "", nil, e.NewSyntax("Expecting SET")
	}
	tkns.Remove()
	isValidSetExpression := false
	// col = value
	for {
		// stop if end of tokens or a WHERE
		if tkns.Len() <= 0 || tkns.Test(t.Where) != "" {
			break
		}
		// Identifier first
		if colName = tkns.Test(t.Ident); colName != "" {
			cd := tab.FindColDef(profile, colName)
			if cd == nil {
				return "", nil, e.NewSyntax(fmt.Sprintf("Invalid Column name: %s does not exist in Table %s", colName, tableName))
			}
			tkns.Remove()
			// Then an EQUAL sign
			if tkns.Test(t.Equal) == "" {
				return "", nil, e.NewSyntax(fmt.Sprintf("Expecting = after column name %s in UPDATE SET", colName))
			}
			tkns.Remove()

			// Get a value
			if tkns.Test(t.Num, t.Quote, t.RWTrue, t.RWFalse) != "" {
				val, err := sqtypes.CreateValueFromToken(*tkns.Peek())
				if err != nil {
					return "", nil, err
				}
				tkns.Remove()
				setCols = append(setCols, colName)
				setVals = append(setVals, val)
				isValidSetExpression = true
				if tkns.Test(t.Comma) != "" {
					tkns.Remove()
				} else {
					break
				}
			} else {
				err = e.NewSyntax(fmt.Sprintf("Expecting a value in SET clause after %s =", colName))
				return "", nil, err
			}
		}

	}
	if !isValidSetExpression {
		return "", nil, e.NewSyntax("Expecting valid SET expression")
	}
	// Optional Where Clause
	if tkns.Len() > 0 && tkns.Test(t.Where) != "" {
		tkns.Remove()
		tkns, cond, err = GetWhereConditions(profile, tkns, tab)
	}

	if !tkns.IsEmpty() {
		return "", nil, e.NewSyntax("Unexpected tokens after SQL command:" + tkns.ToString())
	}

	// get the data
	//	colList := sqtables.NewColListNames(setCols)
	tab.Lock(profile)
	defer tab.Unlock(profile)
	ptrs, err := tab.GetRowPtrs(profile, cond, false)
	if err != nil {
		return "", nil, err
	}
	//Update the rows
	err = tab.UpdateRowsFromPtrs(profile, ptrs, setCols, setVals)
	if err != nil {
		return "", nil, err
	}
	err = redo.Send(redo.NewUpdateRows(tableName, setCols, setVals, ptrs))

	return fmt.Sprintf("Updated %d rows from table", len(ptrs)), nil, err
}
