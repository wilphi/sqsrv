package sqtables_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("sqtables_test.log")
}

func TestColDef(t *testing.T) {
	data := []ColDefData{
		{
			TestName:  "NewColDef with nulls",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: false,
			ExpString: "{col1, INT}",
		},
		{
			TestName:  "NewColDef without nulls",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: true,
			ExpString: "{col1, INT NOT NULL}",
		},
		{
			TestName:         "NewColDef all values",
			ColName:          "col1",
			ColType:          tokens.Int,
			IsNotNull:        true,
			Idx:              5,
			TableName:        "testTab",
			DisplayTableName: true,
			ExpString:        "{testTab.col1, INT NOT NULL}",
		},
		{
			TestName:         "NewColDef do not display tablename",
			ColName:          "col1",
			ColType:          tokens.Int,
			IsNotNull:        true,
			Idx:              5,
			TableName:        "testTab",
			DisplayTableName: false,
			ExpString:        "{col1, INT NOT NULL}",
		},
		{
			TestName:  "NewColDef Merge",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: true,
			MergeCD:   &sqtables.ColDef{ColName: "col1", ColType: tokens.Int, Idx: 12, IsNotNull: false, TableName: "tlist1", DisplayTableName: true},
			ExpCD:     sqtables.ColDef{ColName: "col1", ColType: tokens.Int, Idx: 12, IsNotNull: false, TableName: "tlist1", DisplayTableName: false},
			ExpString: "{col1, INT NOT NULL}",
		},
		{
			TestName:  "NewColDef Merge Error",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: true,
			MergeCD:   &sqtables.ColDef{ColName: "col2", ColType: tokens.Int, Idx: 12, IsNotNull: false, TableName: "tlist1", DisplayTableName: true},
			ExpCD:     sqtables.ColDef{ColName: "col1", ColType: tokens.Int, Idx: 12, IsNotNull: false, TableName: "tlist1", DisplayTableName: false},
			ExpString: "{col1, INT NOT NULL}",
			ExpErr:    "Internal Error: Can't merge ColDef col1, col2",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testColDefFunc(row))

	}

}

type ColDefData struct {
	TestName         string
	ColName          string
	ColType          tokens.TokenID
	TableName        string
	Idx              int
	IsNotNull        bool
	DisplayTableName bool
	MergeCD          *sqtables.ColDef
	ExpCD            sqtables.ColDef
	ExpString        string
	ExpErr           string
}

func testColDefFunc(d ColDefData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		cd := sqtables.NewColDef(d.ColName, d.ColType, d.IsNotNull)
		if d.ColName != cd.ColName || d.ColType != cd.ColType || cd.Idx != -1 || d.IsNotNull != cd.IsNotNull {
			t.Errorf("Created ColDef does not match expected")
			return
		}
		if d.Idx != 0 {
			cd.Idx = d.Idx
		}
		if d.TableName != "" {
			cd.TableName = d.TableName
		}

		cd.DisplayTableName = d.DisplayTableName
		if d.ExpString != cd.String() {
			t.Errorf("String %q does not match expected: %q", cd.String(), d.ExpString)

		}

		bin := sqbin.NewCodec(nil)
		cd.Encode(bin)
		newCd := sqtables.ColDef{}
		newCd.Decode(bin)

		//fmt.Printf(" Original: %v\nRecreated: %v\n", cd, newCd)
		if !reflect.DeepEqual(cd, newCd) {
			t.Error("ColDef encoded/decoded does not match original")
		}
		if d.MergeCD != nil {
			newCD, err := sqtables.MergeColDef(cd, *d.MergeCD)
			if sqtest.CheckErr(t, err, d.ExpErr) {
				return
			}

			if !reflect.DeepEqual(newCD, d.ExpCD) {
				t.Errorf("ColDef Merge: Expected %v does not match actual %v", d.ExpCD, newCD)
			}

		}

	}
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
				t.Errorf("ColList len does not match expected: Actual: %d, Expected: %d", d.CList.Len(), len(d.ColNames))
				return
			}
			if !reflect.DeepEqual(d.ColNames, d.CList.GetColNames()) {
				t.Errorf("ColList ColNames do not match expected: Actual: %v, Expected: %v", d.CList.GetColNames(), d.ColNames)
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
	CList    *sqtables.ColList
	ColNames []string
	ExpErr   string
	profile  *sqprofile.SQProfile
	tables   *sqtables.TableList
}

func TestColListValidate(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	str := "Create table collistValidatetest (col1 int, col2 string, col3 float, col4 bool)"
	tableName, _, err := cmd.CreateTable(profile, tokens.Tokenize(str))
	if err != nil {
		t.Error("Unable to setup table")
		return
	}

	tab, err := sqtables.GetTable(profile, tableName)
	if err != nil {
		t.Error(err)
		return
	}

	if tab == nil {
		t.Error("Unable to get setup table")
		return
	}

	data := []ColListValidateData{
		{
			TestName: "All Cols",
			CList:    sqtables.NewColListNames([]string{"col1", "col4", "col3", "col2"}),
			ColNames: []string{"col1", "col4", "col3", "col2"},
			ExpErr:   "",
			profile:  profile,
			tables:   sqtables.NewTableListFromTableDef(profile, tab),
		},
		{
			TestName: "Invalid Col",
			CList:    sqtables.NewColListNames([]string{"col1", "col4", "col3", "col2", "colX"}),
			ExpErr:   "Error: Column \"colX\" not found in Table(s): collistvalidatetest",
			profile:  profile,
			tables:   sqtables.NewTableListFromTableDef(profile, tab),
		},
		{
			TestName: "Count Col",
			CList:    sqtables.NewColListNames([]string{"COUNT"}),
			ExpErr:   "",
			profile:  profile,
			tables:   sqtables.NewTableListFromTableDef(profile, tab),
		},
		{
			TestName: "Count Col + extra col",
			CList:    sqtables.NewColListNames([]string{"COUNT", "col1"}),
			ExpErr:   "Error: The function Count can not be used with Columns",
			profile:  profile,
			tables:   sqtables.NewTableListFromTableDef(profile, tab),
		},
		{
			TestName: "Coldef ColList Cols",
			CList:    sqtables.NewColListDefs([]sqtables.ColDef{sqtables.NewColDef("col1", tokens.Int, false), sqtables.NewColDef("col2", tokens.String, false)}),
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

func TestColListFindColDef(t *testing.T) {
	col1CD := sqtables.NewColDef("col1", tokens.Int, false)
	colList := sqtables.NewColListDefs([]sqtables.ColDef{col1CD, sqtables.NewColDef("col2", tokens.String, false)})

	t.Run("Found ColDef", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		cd := colList.FindColDef("col1")
		if cd.String() != col1CD.String() {
			t.Errorf("Did not find expected ColDef: Actual: %s, Expected: %s", cd.String(), col1CD.String())
		}
	})

	t.Run("No ColDef", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		cd := colList.FindColDef("colX")
		if cd != nil {
			t.Errorf("ColDef found unexpectedly: %s", cd.String())
		}
	})
}
