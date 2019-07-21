package sqtables_test

import (
	"fmt"
	"log"
	"reflect"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

func TestColDef(t *testing.T) {
	t.Run("CreateColDef Null", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		colName := "col1"
		colType := tokens.TypeInt
		isNotNull := false
		cd := sqtables.CreateColDef(colName, colType, isNotNull)
		if colName != cd.ColName || colType != cd.ColType || cd.Idx != -1 || isNotNull != cd.IsNotNull {
			t.Errorf("Created ColDef does not match expected")
			return
		}
		str := "{" + colName + ", " + colType + "}"
		if str != cd.ToString() {
			t.Errorf("ToString %q does not match expected: %q", cd.ToString(), str)

		}

	})

	t.Run("CreateColDef Not Null", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		colName := "col1"
		colType := tokens.TypeInt
		isNotNull := true
		cd := sqtables.CreateColDef(colName, colType, isNotNull)
		if colName != cd.ColName || colType != cd.ColType || cd.Idx != -1 || isNotNull != cd.IsNotNull {
			t.Errorf("Created ColDef does not match expected")
			return
		}
		str := "{" + colName + ", " + colType + ", NOT NULL}"
		if str != cd.ToString() {
			t.Errorf("ToString %q does not match expected: %q", cd.ToString(), str)

		}
	})

	t.Run("ColDef Encode/Decode", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()

		cd := sqtables.CreateColDef("col1", tokens.TypeString, true)
		cd.Idx = 3

		bin := sqbin.NewCodec(nil)
		cd.Encode(bin)
		newCd := sqtables.ColDef{}
		newCd.Decode(bin)

		if !reflect.DeepEqual(cd, newCd) {
			t.Error("ColDef encoded/decoded does not match original")
		}

	})
}

func testColListValidateFunc(d ColListValidateData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		err := d.CList.ValidateTable(d.profile, d.tab)
		if err != nil {
			log.Println(err.Error())
			if d.ExpErr == "" {
				t.Errorf("Unexpected Error in test: %s", err.Error())
				return
			}
			if d.ExpErr != err.Error() {
				t.Errorf("Expecting Error %s but got: %s", d.ExpErr, err.Error())
				return
			}
			return
		}

	}
}

type ColListValidateData struct {
	TestName string
	CList    sqtables.ColList
	ExpErr   string
	profile  *sqprofile.SQProfile
	tab      *sqtables.TableDef
}

func TestColListValidate(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	str := "Create table collistValidatetest (col1 int, col2 string, col3 float, col4 bool)"
	tableName, _, err := cmd.CreateTable(profile, tokens.Tokenize(str))
	if err != nil {
		t.Error("Unable to setup table")
		return
	}

	tab := sqtables.GetTable(profile, tableName)
	if tab == nil {
		t.Error("Unable to get setup table")
		return
	}

	data := []ColListValidateData{
		{
			TestName: "All Cols",
			CList:    sqtables.NewColListNames([]string{"col1", "col4", "col3", "col2"}),
			ExpErr:   "",
			profile:  profile,
			tab:      tab,
		},
		{
			TestName: "Invalid Col",
			CList:    sqtables.NewColListNames([]string{"col1", "col4", "col3", "col2", "colX"}),
			ExpErr:   "Error: Table collistvalidatetest does not have a column named colX",
			profile:  profile,
			tab:      tab,
		},
		{
			TestName: "Count Col",
			CList:    sqtables.NewColListNames([]string{"COUNT"}),
			ExpErr:   "",
			profile:  profile,
			tab:      tab,
		},
		{
			TestName: "Count Col + extra col",
			CList:    sqtables.NewColListNames([]string{"COUNT", "col1"}),
			ExpErr:   "Error: The function Count can not be used with Columns",
			profile:  profile,
			tab:      tab,
		},
		{
			TestName: "Coldef ColList Cols",
			CList:    sqtables.NewColListDefs([]sqtables.ColDef{sqtables.CreateColDef("col1", tokens.TypeInt, false), sqtables.CreateColDef("col2", tokens.TypeString, false)}),
			ExpErr:   "",
			profile:  profile,
			tab:      tab,
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testColListValidateFunc(row))

	}

}
