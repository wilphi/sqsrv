package column_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("column_test.log")
}

///////////////////////////////////////////////////////////////////////////////
func testColListValidateFunc(d ColListValidateData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		err := d.CList.Validate(d.profile, d.tables)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		// Check ColNames
		if d.ColNames != nil {
			if d.CList.Len() != len(d.ColNames) {
				t.Errorf("column.List len does not match expected: Actual: %d, Expected: %d", d.CList.Len(), len(d.ColNames))
				return
			}
			if !reflect.DeepEqual(d.ColNames, d.CList.GetColNames()) {
				t.Errorf("column.List ColNames do not match expected: Actual: %v, Expected: %v", d.CList.GetColNames(), d.ColNames)
				return
			}
		}
		// Double validate
		err = d.CList.Validate(d.profile, d.tables)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

	}
}

type ColListValidateData struct {
	TestName string
	CList    *column.List
	ColNames []string
	ExpErr   string
	profile  *sqprofile.SQProfile
	tables   *sqtables.TableList
}

func TestColListValidate(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	tableName := "collistValidatetest"
	tab := sqtables.CreateTableDef(tableName,
		column.NewDef("col1", tokens.Int, false),
		column.NewDef("col2", tokens.String, false),
		column.NewDef("col3", tokens.Float, false),
		column.NewDef("col4", tokens.Bool, false),
	)
	err := sqtables.CreateTable(profile, tab)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	data := []ColListValidateData{
		{
			TestName: "All Cols",
			CList:    column.NewListNames([]string{"col1", "col4", "col3", "col2"}),
			ColNames: []string{"col1", "col4", "col3", "col2"},
			ExpErr:   "",
			profile:  profile,
			tables:   sqtables.NewTableListFromTableDef(profile, tab),
		},
		{
			TestName: "Invalid Col",
			CList:    column.NewListNames([]string{"col1", "col4", "col3", "col2", "colX"}),
			ExpErr:   "Error: Column \"colX\" not found in Table(s): collistvalidatetest",
			profile:  profile,
			tables:   sqtables.NewTableListFromTableDef(profile, tab),
		},
		{
			TestName: "Count Col",
			CList:    column.NewListNames([]string{"COUNT"}),
			ExpErr:   "",
			profile:  profile,
			tables:   sqtables.NewTableListFromTableDef(profile, tab),
		},
		{
			TestName: "Count Col + extra col",
			CList:    column.NewListNames([]string{"COUNT", "col1"}),
			ExpErr:   "Error: The function Count can not be used with Columns",
			profile:  profile,
			tables:   sqtables.NewTableListFromTableDef(profile, tab),
		},
		{
			TestName: "Ref column.List Cols",
			CList:    column.NewListRefs([]column.Ref{column.NewRef("col1", tokens.Int, false), column.NewRef("col2", tokens.String, false)}),
			ExpErr:   "",
			profile:  profile,
			tables:   sqtables.NewTableListFromTableDef(profile, tab),
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testColListValidateFunc(row))

	}

}

////////////////////////////////////////////////////////////////////////////////////////////////

func TestColListFindRef(t *testing.T) {
	col1CD := column.NewRef("col1", tokens.Int, false)
	colList := column.NewListRefs([]column.Ref{col1CD, column.NewRef("col2", tokens.String, false)})

	t.Run("Found column.Ref", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		cd := colList.FindRef("col1")
		if cd.String() != col1CD.String() {
			t.Errorf("Did not find expected column.Ref: Actual: %s, Expected: %s", cd.String(), col1CD.String())
		}
	})

	t.Run("No column.Ref", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		cd := colList.FindRef("colX")
		if cd != nil {
			t.Errorf("column.Ref found unexpectedly: %s", cd.String())
		}
	})
}

func TestNewListMethods(t *testing.T) {

	t.Run("NewListRefs", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")
		refarray := []column.Ref{column.NewRef("col1", tokens.Int, false), column.NewRef("col2", tokens.String, false)}
		colListRefs := column.NewListRefs(refarray)

		if !reflect.DeepEqual(refarray, colListRefs.GetRefs()) {
			t.Errorf("Expected Ref array (%v) does not match GetRefs from collist (%v)", refarray, colListRefs.GetRefs())
		}
	})

	t.Run("NewListDefs", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")
		defarray := []column.Def{column.NewDef("col1", tokens.Int, false), column.NewDef("col2", tokens.String, false)}
		colListDefs := column.NewListDefs(defarray)

		refarray := make([]column.Ref, len(defarray))
		for i, d := range defarray {
			refarray[i] = d.Ref()
		}

		if !reflect.DeepEqual(refarray, colListDefs.GetRefs()) {
			t.Errorf("Expected Ref array (%v) does not match GetRefs from collist (%v)", refarray, colListDefs.GetRefs())
		}
	})

	t.Run("NewListNames", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")
		colNames := []string{"col1", "table1.col2", "col3"}
		colListNames := column.NewListNames(colNames)

		refarray := make([]column.Ref, len(colNames))
		for i, name := range colNames {
			s := strings.Split(name, ".")
			switch len(s) {
			case 1:
				refarray[i] = column.Ref{ColName: s[0]}
			case 2:
				refarray[i] = column.Ref{ColName: s[1], TableName: moniker.New(s[0], "")}
			}
		}

		if !reflect.DeepEqual(refarray, colListNames.GetRefs()) {
			t.Errorf("Expected Ref array (%v) does not match GetRefs from collist (%v)", refarray, colListNames.GetRefs())
		}
	})

}
