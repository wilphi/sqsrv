package cmd_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("cmd_test.log")
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func testGroupByFunc(profile *sqprofile.SQProfile, d GroupByData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, false)

		tkns := tokens.Tokenize(d.Command)

		eList, err := cmd.GroupByClause(tkns)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if d.ExpEList != nil {
			if !reflect.DeepEqual(eList, d.ExpEList) {
				t.Errorf("Expected Order By expressions do not match actual expressions\n\tExpect: %s\n\tActual: %s", d.ExpEList.ToString(), eList.ToString())
			}
		}
	}
}

type GroupByData struct {
	TestName string
	Command  string
	ExpErr   string
	ExpEList *sqtables.ExprList
}

func TestGroupBy(t *testing.T) {

	//make sure table exists for testing
	profile := sqprofile.CreateSQProfile()

	data := []GroupByData{
		{
			TestName: "Empty string",
			Command:  "",
			ExpErr:   "Syntax Error: GROUP missing BY",
		},
		{
			TestName: "By only",
			Command:  "By",
			ExpErr:   "Syntax Error: No expressions defined for GROUP BY clause",
		},
		{
			TestName: "GROUP By only",
			Command:  "GROUP By",
			ExpErr:   "Syntax Error: No expressions defined for GROUP BY clause",
		},
		{
			TestName: "GROUP By",
			Command:  "GROUP By col1, col2",
			ExpErr:   "",
		},
		{
			TestName: "GROUP By count()",
			Command:  "GROUP By col1, count()",
			ExpErr:   "Syntax Error: GROUP BY clause expression can't contain aggregate functions: COUNT()",
		},
		{
			TestName: "GROUP By min()",
			Command:  "GROUP By col1, min(col2)",
			ExpErr:   "Syntax Error: GROUP BY clause expression can't contain aggregate functions: MIN(col2)",
		},
		{
			TestName: "GROUP By max()",
			Command:  "GROUP By col1, max(col2)",
			ExpErr:   "Syntax Error: GROUP BY clause expression can't contain aggregate functions: MAX(col2)",
		},
		{
			TestName: "GROUP By sum()",
			Command:  "GROUP By col1, sum(col2)",
			ExpErr:   "Syntax Error: GROUP BY clause expression can't contain aggregate functions: SUM(col2)",
		},
		{
			TestName: "GROUP By count()",
			Command:  "GROUP By col1, avg(col2)",
			ExpErr:   "Syntax Error: GROUP BY clause expression can't contain aggregate functions: AVG(col2)",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGroupByFunc(profile, row))

	}
}
