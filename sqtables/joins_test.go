package sqtables_test

import (
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
func TestMiscJoinRow(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	// Setup Data
	ptr12 := sqptr.SQPtr(12)
	tName := moniker.New("jointable", "")
	row := sqtables.JoinRow{
		Ptr: ptr12,
		Vals: []sqtypes.Value{
			sqtypes.NewSQInt(5),
			sqtypes.NewSQString("test1"),
			sqtypes.NewSQBool(true),
			sqtypes.NewSQString("test2"),
		},
		TableName: tName,
	}

	col := column.NewRef("col1", tokens.String, false)

	t.Run("joinRow is valid RowInterface", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		var i sqtables.RowInterface
		i = &row
		_, ok := i.(sqtables.RowInterface)
		if !ok {
			t.Error("Row is not a RowInterface")
			return
		}
	})
	t.Run("GetTableName", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if row.GetTableName(profile) != tName.Name() {
			t.Error("GetTableName did not match expected value")
			return
		}
	})
	t.Run("GetPtr", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if row.GetPtr(profile) != ptr12 {
			t.Error("GetPtr did not match expected value")
			return
		}
	})
	t.Run("GetIdxVal idx=-1", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		errTxt := "Error: Invalid index (-1) for row. Data len = 4"
		_, err := row.GetIdxVal(profile, -1)

		if err.Error() != errTxt {
			t.Errorf("Expected err %q did not match actual error %q", errTxt, err)
			return
		}
	})
	t.Run("GetIdxVal idx=4", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		errTxt := "Error: Invalid index (4) for row. Data len = 4"
		_, err := row.GetIdxVal(profile, 4)

		if err.Error() != errTxt {
			t.Errorf("Expected err %q did not match actual error %q", errTxt, err)
			return
		}
	})
	t.Run("GetIdxVal idx=1", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		errTxt := ""
		expVal := sqtypes.NewSQString("test1")
		v, err := row.GetIdxVal(profile, 1)

		if err != nil && err.Error() != errTxt {
			t.Errorf("Expected err %q did not match actual error %q", errTxt, err)
			return
		}
		if !v.Equal(expVal) {
			t.Errorf("Expected Value %s does not match actual value %s", expVal.String(), v.String())
		}
	})

	t.Run("GetColData idx=-1", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		errTxt := "Error: Invalid index (-1) for Column in row. Col len = 4"
		col.Idx = -1
		_, err := row.GetColData(profile, &col)

		if err.Error() != errTxt {
			t.Errorf("Expected err %q did not match actual error %q", errTxt, err)
			return
		}
	})
	t.Run("GetColData idx=4", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		errTxt := "Error: Invalid index (4) for Column in row. Col len = 4"
		col.Idx = 4
		_, err := row.GetColData(profile, &col)

		if err != nil {
			if err.Error() != errTxt {
				t.Errorf("Expected err %q did not match actual error %q", errTxt, err)
				return
			}
			return
		}
	})
	t.Run("GetColData idx=1", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		errTxt := ""
		expVal := sqtypes.NewSQString("test1")
		col.Idx = 1
		v, err := row.GetColData(profile, &col)

		if err != nil && err.Error() != errTxt {
			t.Errorf("Expected err %q did not match actual error %q", errTxt, err)
			return
		}
		if !v.Equal(expVal) {
			t.Errorf("Expected Value %s does not match actual value %s", expVal.String(), v.String())
		}
	})

}

func TestMiscNullRow(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	// Setup Data
	tName := moniker.New("nulltable", "")
	row := sqtables.NullRow{
		TableName: tName,
	}

	col := column.NewRef("col1", tokens.String, false)

	t.Run("NullRow is valid RowInterface", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		var i sqtables.RowInterface
		i = &row
		_, ok := i.(sqtables.RowInterface)
		if !ok {
			t.Error("Row is not a RowInterface")
			return
		}
	})
	t.Run("GetTableName", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if row.GetTableName(profile) != tName.Name() {
			t.Error("GetTableName did not match expected value")
			return
		}
	})
	t.Run("GetPtr", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if row.GetPtr(profile) != 0 {
			t.Error("GetPtr did not match expected value")
			return
		}
	})
	t.Run("GetIdxVal idx=-1", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		val, err := row.GetIdxVal(profile, -1)

		if !val.IsNull() {
			t.Errorf("Value is not null")
			return
		}
		if err != nil {
			t.Errorf("Unexpected err %q ", err)
			return
		}
	})
	t.Run("GetIdxVal idx=4", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		val, err := row.GetIdxVal(profile, 4)

		if !val.IsNull() {
			t.Errorf("Value is not null")
			return
		}
		if err != nil {
			t.Errorf("Unexpected err %q ", err)
			return
		}
	})
	t.Run("GetIdxVal idx=1", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		v, err := row.GetIdxVal(profile, 1)

		if err != nil {
			t.Errorf("Unexpected err %q ", err)
			return
		}

		if !v.IsNull() {
			t.Errorf("Value is not null")
			return
		}
	})

	t.Run("GetColData idx=-1", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		val, err := row.GetColData(profile, &col)

		if !val.IsNull() {
			t.Errorf("Value is not null")
			return
		}
		if err != nil {
			t.Errorf("Unexpected err %q ", err)
			return
		}
	})
	t.Run("GetColData idx=4", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		val, err := row.GetColData(profile, &col)

		if !val.IsNull() {
			t.Errorf("Value is not null")
			return
		}
		if err != nil {
			t.Errorf("Unexpected err %q ", err)
			return
		}
	})
	t.Run("GetColData idx=1", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		val, err := row.GetColData(profile, &col)

		if !val.IsNull() {
			t.Errorf("Value is not null")
			return
		}
		if err != nil {
			t.Errorf("Unexpected err %q ", err)
			return
		}

	})

}
