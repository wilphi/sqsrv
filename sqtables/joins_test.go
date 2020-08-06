package sqtables_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("sqtables_test.log")
}

type JoinRowTypesData struct {
	TestName  string
	Row       sqtables.RowInterface
	ExpVals   []sqtypes.Raw
	TableName string
	Ptr       sqptr.SQPtr
	Idx       int
	IdxVal    sqtypes.Raw
	IdxErr    string
	Col       column.Ref
	ColVal    sqtypes.Raw
	ColErr    string
}

func testJoinRowTypesFunc(d JoinRowTypesData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()

		//Check tableName
		if d.Row.GetTableName(profile) != d.TableName {
			t.Errorf("Actual TableName: %q does not match Expected: %q", d.Row.GetTableName(profile), d.TableName)
			return
		}

		if d.Row.GetPtr(profile) != d.Ptr {
			t.Errorf("Actual Ptr: %d does not match Expected: %d", d.Row.GetPtr(profile), d.Ptr)
			return
		}

		expVal := sqtypes.RawValue(d.IdxVal)
		v, err := d.Row.IdxVal(profile, d.Idx)
		if !sqtest.CheckErr(t, err, d.IdxErr) {

			if !v.Equal(expVal) {
				if !(expVal.IsNull() && v.IsNull()) {
					t.Errorf("IdxVal: Expected Value %s does not match actual value %s", expVal.String(), v.String())
					// Do not return here
				}
			}
		}
		col := d.Col
		col.Idx = d.Idx

		v, err = d.Row.ColVal(profile, &col)
		if !sqtest.CheckErr(t, err, d.ColErr) {

			if !v.Equal(expVal) {
				if !(expVal.IsNull() && v.IsNull()) {
					t.Errorf("ColVal: Expected Value %s does not match actual value %s", expVal.String(), v.String())
					// do not return here
				}
			}
		}

		if d.Row.IsDeleted(profile) != false {
			t.Error("IsDeleted() must return false")
			return
		}
		actVals := d.Row.GetVals(profile)
		expVals := sqtypes.CreateValueArrayFromRaw(d.ExpVals)

		if !reflect.DeepEqual(actVals, expVals) {
			if !(len(actVals) == 0 && len(expVals) == 0) {
				t.Error("GetVals does not match Expected Vals")
				return
			}
		}

	}

}
func TestJoinRowTypes(t *testing.T) {
	ptr12 := sqptr.SQPtr(12)
	tName := moniker.New("jointable", "")
	nName := moniker.New("nulltable", "")

	data := []JoinRowTypesData{
		{
			TestName: "joinRow idx=-1",
			Row: &sqtables.JoinRow{
				Ptr: ptr12,
				Vals: []sqtypes.Value{
					sqtypes.NewSQInt(5),
					sqtypes.NewSQString("test1"),
					sqtypes.NewSQBool(true),
					sqtypes.NewSQString("test2"),
				},
				TableName: tName,
			},
			ExpVals:   []sqtypes.Raw{5, "test1", true, "test2"},
			TableName: "jointable",
			Ptr:       ptr12,
			Idx:       -1,
			IdxVal:    nil,
			IdxErr:    "Error: Invalid index (-1) for row. Data len = 4",
			Col:       column.NewRef("col1", tokens.String, false),
			ColErr:    "Error: Invalid index (-1) for Column in row. Col len = 4",
		},
		{
			TestName: "joinRow idx=4",
			Row: &sqtables.JoinRow{
				Ptr: ptr12,
				Vals: []sqtypes.Value{
					sqtypes.NewSQInt(5),
					sqtypes.NewSQString("test1"),
					sqtypes.NewSQBool(true),
					sqtypes.NewSQString("test2"),
				},
				TableName: tName,
			},
			ExpVals:   []sqtypes.Raw{5, "test1", true, "test2"},
			TableName: "jointable",
			Ptr:       ptr12,
			Idx:       4,
			IdxVal:    nil,
			IdxErr:    "Error: Invalid index (4) for row. Data len = 4",
			Col:       column.NewRef("col1", tokens.String, false),
			ColErr:    "Error: Invalid index (4) for Column in row. Col len = 4",
		},
		{
			TestName: "joinRow idx=1",
			Row: &sqtables.JoinRow{
				Ptr: ptr12,
				Vals: []sqtypes.Value{
					sqtypes.NewSQInt(5),
					sqtypes.NewSQString("test1"),
					sqtypes.NewSQBool(true),
					sqtypes.NewSQString("test2"),
				},
				TableName: tName,
			},
			ExpVals:   []sqtypes.Raw{5, "test1", true, "test2"},
			TableName: "jointable",
			Ptr:       ptr12,
			Idx:       1,
			IdxVal:    "test1",
			IdxErr:    "",
			Col:       column.NewRef("col1", tokens.String, false),
			ColErr:    "",
		},
		{
			TestName: "NullRow idx=-1",
			Row: &sqtables.NullRow{
				TableName: nName,
			},
			ExpVals:   nil,
			TableName: "nulltable",
			Ptr:       0,
			Idx:       -1,
			IdxVal:    nil,
			IdxErr:    "",
			Col:       column.NewRef("col1", tokens.String, false),
			ColErr:    "",
		},
		{
			TestName: "NullRow idx=4",
			Row: &sqtables.NullRow{
				TableName: nName,
			},
			ExpVals:   nil,
			TableName: "nulltable",
			Ptr:       0,
			Idx:       4,
			IdxVal:    nil,
			IdxErr:    "",
			Col:       column.NewRef("col1", tokens.String, false),
			ColErr:    "",
		},
		{
			TestName: "NullRow idx=1",
			Row: &sqtables.NullRow{
				TableName: nName,
			},
			ExpVals:   nil,
			TableName: "nulltable",
			Ptr:       0,
			Idx:       1,
			IdxVal:    nil,
			IdxErr:    "",
			Col:       column.NewRef("col1", tokens.String, false),
			ColErr:    "",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testJoinRowTypesFunc(row))

	}

}
