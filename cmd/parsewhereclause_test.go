package cmd_test

import (
	"fmt"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("cmd_test.log")
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////

type ParseWhereClauseData struct {
	TestName   string
	Terminator tokens.TokenID
	Command    string
	ExpErr     string
	ExpExpr    string
	ValuesOnly bool
}

func testParseWhereClauseFunc(d ParseWhereClauseData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		tkns := tokens.Tokenize(d.Command)

		actExpr, err := cmd.ParseWhereClause(tkns, false, d.Terminator)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if d.ExpExpr != actExpr.Name() {
			t.Errorf("Expected Expressions %s do not match actual Expressions %s", d.ExpExpr, actExpr.Name())
			return
		}
	}
}

func TestParseWhereClause(t *testing.T) {

	data := []ParseWhereClauseData{
		{
			TestName:   "Full Where expression",
			Terminator: tokens.Order,
			Command:    "col1 = -1 and col2 = \"test\" or col1=3 and col2 = \"nat\"",
			ExpErr:     "",
			ExpExpr:    "(((col1=-1)AND(col2=test))OR((col1=3)AND(col2=nat)))",
		},
		{
			TestName:   "Where with Col comparison same tables",
			Terminator: tokens.Order,
			Command:    "a.col1 = a.col2 ",
			ExpErr:     "",
			ExpExpr:    "(a.col1=a.col2)",
		},
		{
			TestName:   "Where with Col comparison different tables",
			Terminator: tokens.Order,
			Command:    "a.col1 = b.col2 ",
			ExpErr:     "Syntax Error: To join tables use the On condition in the From Clause",
			ExpExpr:    "",
		},
		{
			TestName:   "Where with Col comparison different tables left path",
			Terminator: tokens.Order,
			Command:    "a.col1 = b.col2 and col1=2 ",
			ExpErr:     "Syntax Error: To join tables use the On condition in the From Clause",
			ExpExpr:    "",
		},
		{
			TestName:   "Where with Col comparison different tables right path",
			Terminator: tokens.Order,
			Command:    "col1 = 3 or a.col1 = b.col2",
			ExpErr:     "Syntax Error: To join tables use the On condition in the From Clause",
			ExpExpr:    "",
		},
		/*		{
					TestName:   "Full expression <= =>, !=",
					Terminator: tokens.Order,
					Command:    "col1 <= 1 and col2 != \"test\" or col1>=3 and col2 = \"nat\"",
					ExpErr:     "",
					ExpExpr:    "(((col1<=1)AND(col2!=test))OR((col1>=3)AND(col2=nat)))",
				},
				{
					TestName:   "Expression with FLOAT function",
					Terminator: tokens.Order,
					Command:    "FLOAT(col1) <= 1.0",
					ExpErr:     "",
					ExpExpr:    "(FLOAT(col1)<=1)",
				},*/
		{
			TestName:   "Expression with FLOAT error no (",
			Terminator: tokens.Order,
			Command:    "FLOAT col1  <= 1.0",
			ExpErr:     "Syntax Error: Function FLOAT must be followed by (",
			ExpExpr:    "(FLOAT(col1)<=1)",
		},
		/*		{
					TestName:   "Expression with FLOAT error no )",
					Terminator: tokens.Order,
					Command:    "FLOAT(col1  <= 1.0",
					ExpErr:     "Syntax Error: Function FLOAT is missing ) after expression",
					ExpExpr:    "(FLOAT(col1)<=1)",
				},
				{
					TestName:   "Expression with FLOAT error no expression in()",
					Terminator: tokens.Order,
					Command:    "FLOAT() <= 1.0",
					ExpErr:     "Syntax Error: Function FLOAT is missing an expression between ( and )",
					ExpExpr:    "(FLOAT(col1)<=1)",
				},
				{
					TestName:   "Expression with FLOAT function with err",
					Terminator: tokens.Order,
					Command:    "FLOAT(float) <= 1.0",
					ExpErr:     "Syntax Error: Function FLOAT must be followed by (",
					ExpExpr:    "(FLOAT(col1)<=1)",
				},
		*/
		{
			TestName:   "Count Expression",
			Terminator: tokens.From,
			Command:    "count()",
			ExpErr:     "Syntax Error: Aggregate functions (COUNT) are not allowed in Where clause",
			ExpExpr:    "COUNT()",
		},

		{
			TestName:   "Count With Expression",
			Terminator: tokens.From,
			Command:    "count(x=1)",
			ExpErr:     "Syntax Error: Aggregate functions (COUNT) are not allowed in Where clause",
			ExpExpr:    "COUNT((x=1))",
		},
		/*		{
					TestName:   "Err Expression",
					Terminator: tokens.From,
					Command:    "a+ by",
					ExpErr:     "Syntax Error: Invalid expression: Unable to find a value or column near BY",
					ExpExpr:    "count()",
				},
				{
					TestName:   "TableName.Col ",
					Terminator: tokens.From,
					Command:    "tablea.col FROM",
					ExpErr:     "",
					ExpExpr:    "tablea.col",
				},
				{
					TestName:   "TableName. missing col ",
					Terminator: tokens.From,
					Command:    "tablea. FROM",
					ExpErr:     "Syntax Error: Expecting column after tablea.",
					ExpExpr:    "tablea.col",
				},
				{
					TestName:   "int Without Expression and )",
					Terminator: tokens.From,
					Command:    "int(",
					ExpErr:     "Syntax Error: Function INT is missing an expression followed by )",
					ExpExpr:    "INT()",
				},
				{
					TestName:   "int Without Expression",
					Terminator: tokens.From,
					Command:    "int()",
					ExpErr:     "Syntax Error: Function INT is missing an expression between ( and )",
					ExpExpr:    "INT()",
				}, */
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testParseWhereClauseFunc(row))

	}
}
