package sqtables_test

import (
	"testing"

	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("sqtables_test.log")
}

type UpdateRowData struct {
	TestName string
	Row      *sqtables.RowDef
	Cols     []string
	Vals     []sqtypes.Raw
	ExpVals  []sqtypes.Raw
	ExpErr   string
}

func testUpdateRowFunc(profile *sqprofile.SQProfile, r *UpdateRowData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		ExpVals := sqtypes.CreateValueArrayFromRaw(r.ExpVals)
		vals := sqtypes.CreateValueArrayFromRaw(r.Vals)
		err := r.Row.UpdateRow(profile, r.Cols, vals)
		if sqtest.CheckErr(t, err, r.ExpErr) {
			return
		}

		// make sure the data matches expected
		for i, val := range r.Row.Data {

			if (val.IsNull() && !ExpVals[i].IsNull()) || (!val.IsNull() && ExpVals[i].IsNull()) {
				t.Errorf("Returned value %q does not match expected %q", val.String(), ExpVals[i].String())
				return
			}

			if !(val.IsNull() && ExpVals[i].IsNull()) {
				if !val.Equal(ExpVals[i]) {
					t.Errorf("Returned value %q does not match expected %q", val.String(), ExpVals[i].String())
					return
				}
			}
		}

	}
}

func TestUpdateRow(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	tableName := "updaterowtest"
	testT := sqtables.CreateTableDef(tableName,
		sqtables.NewColDef("col1", tokens.Int, true),
		sqtables.NewColDef("col2", tokens.String, false),
		sqtables.NewColDef("col3", tokens.Int, false),
		sqtables.NewColDef("col4", tokens.String, true),
	)
	err := sqtables.CreateTable(profile, testT)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	tables := sqtables.NewTableListFromTableDef(profile, testT)
	dsData, err := sqtables.NewDataSet(profile, tables, sqtables.ColsToExpr(testT.GetCols(profile)), nil)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
		{1, "test one2", 21, "test one4"},
		{2, "test two2", 22, "test two4"},
		{3, "test three2", 23, "test three4"},
		{4, "test four2", 24, "test four4"},
	})
	_, err = testT.AddRows(profile, dsData)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}

	row1, err := sqtables.CreateRow(profile, 0, testT, testT.GetColNames(profile), sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{5, "Test Data 0", nil, "Original"}))
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	rowD, err := sqtables.CreateRow(profile, 0, testT, testT.GetColNames(profile), sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{6, "Test Data 0", nil, "Originald"}))
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}
	rowD.Delete(profile)

	testData := []UpdateRowData{
		{
			TestName: "Disordered Columns",
			Row:      row1,
			Cols:     []string{"col2", "col1", "col4", "col3"},
			Vals:     []sqtypes.Raw{"test Five2", 5, "test Five4", 25},
			ExpVals:  []sqtypes.Raw{5, "test Five2", 25, "test Five4"},
			ExpErr:   "",
		},
		{
			TestName: "Null in Null Columns",
			Row:      row1,
			Cols:     []string{"col1", "col2", "col3", "col4"},
			Vals:     []sqtypes.Raw{5, nil, 25, "test Five4"},
			ExpVals:  []sqtypes.Raw{5, nil, 25, "test Five4"},
			ExpErr:   "",
		},
		{
			TestName: "Null in Not Null Columns",
			Row:      row1,
			Cols:     []string{"col1", "col2", "col3", "col4"},
			Vals:     []sqtypes.Raw{nil, nil, 25, "test Five4"},
			ExpVals:  []sqtypes.Raw{nil, nil, 25, "test Five4"},
			ExpErr:   "Error: Column \"col1\" in Table \"updaterowtest\" can not be NULL",
		},
		{
			TestName: "To many Columns",
			Row:      row1,
			Cols:     []string{"col1", "col2", "col3", "col4", "col5"},
			Vals:     []sqtypes.Raw{5, "test Five2", 25, "test Five4", nil},
			ExpVals:  []sqtypes.Raw{5, "test Five2", 25, "test Five4", nil},
			ExpErr:   "Error: Column (col5) does not exist in table (updaterowtest)",
		},
		{
			TestName: "Unknown Columns",
			Row:      row1,
			Cols:     []string{"col1", "col2", "col3", "col5"},
			Vals:     []sqtypes.Raw{5, "test Five2", 25, nil},
			ExpVals:  []sqtypes.Raw{5, "test Five2", 25, nil},
			ExpErr:   "Error: Column (col5) does not exist in table (updaterowtest)",
		},
		{
			TestName: "Only Not Null Columns",
			Row:      row1,
			Cols:     []string{"col1", "col4"},
			Vals:     []sqtypes.Raw{5, "test Five4"},
			ExpVals:  []sqtypes.Raw{5, "test Five2", 25, "test Five4"},
			ExpErr:   "",
		},
		{
			TestName: "Type Mismatch",
			Row:      row1,
			Cols:     []string{"col1", "col2", "col3", "col4"},
			Vals:     []sqtypes.Raw{true, nil, 25, "test Five4"},
			ExpErr:   "Error: Type Mismatch: Column col1 in Table updaterowtest has a type of INT, Unable to set value of type BOOL",
		},
		{
			TestName: "No Value for Not Null Columns",
			Row:      row1,
			Cols:     []string{"col2", "col3", "col4"},
			Vals:     []sqtypes.Raw{nil, 25, "test Five4"},
			ExpVals:  []sqtypes.Raw{5, nil, 25, "test Five4"},
			ExpErr:   "",
		},
		{
			TestName: "Invalid Params len of col!=Vals",
			Row:      row1,
			Cols:     []string{"col2", "col3", "col4"},
			Vals:     []sqtypes.Raw{25, "test Five4"},
			ExpErr:   "Error: The Number of Columns (3) does not match the number of Values (2)",
		}}

	for _, rw := range testData {
		t.Run(rw.TestName, testUpdateRowFunc(profile, &rw))
	}
}

type CreateRowData struct {
	TestName string
	RowPtr   sqptr.SQPtr
	Tab      *sqtables.TableDef
	Cols     []string
	Vals     []sqtypes.Raw
	ExpVals  []sqtypes.Raw
	ExpErr   string
}

func testCreateRowFunc(profile *sqprofile.SQProfile, r *CreateRowData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		ExpVals := sqtypes.CreateValueArrayFromRaw(r.ExpVals)
		row, err := sqtables.CreateRow(profile, r.RowPtr, r.Tab, r.Cols, sqtypes.CreateValueArrayFromRaw(r.Vals))
		if sqtest.CheckErr(t, err, r.ExpErr) {
			return
		}

		// make sure the data matches expected
		for i, val := range row.Data {

			if (val.IsNull() && !ExpVals[i].IsNull()) || (!val.IsNull() && ExpVals[i].IsNull()) {
				t.Errorf("Returned value %q does not match expected %q", val.String(), ExpVals[i].String())
				return
			}

			if !(val.IsNull() && ExpVals[i].IsNull()) {
				if !val.Equal(ExpVals[i]) {
					t.Errorf("Returned value %q does not match expected %q", val.String(), ExpVals[i].String())
					return
				}
			}
		}

	}
}

func TestCreateRow(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	tableName := "createrowtest"
	testT := sqtables.CreateTableDef(tableName,
		sqtables.NewColDef("col1", tokens.Int, true),
		sqtables.NewColDef("col2", tokens.String, false),
		sqtables.NewColDef("col3", tokens.Int, false),
		sqtables.NewColDef("col4", tokens.String, true),
	)
	err := sqtables.CreateTable(profile, testT)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	tables := sqtables.NewTableListFromTableDef(profile, testT)
	dsData, err := sqtables.NewDataSet(profile, tables, sqtables.ColsToExpr(testT.GetCols(profile)), nil)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
		{1, "test one2", 21, "test one4"},
		{2, "test two2", 22, "test two4"},
		{3, "test three2", 23, "test three4"},
		{4, "test four2", 24, "test four4"},
	})
	_, err = testT.AddRows(profile, dsData)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}

	testData := []CreateRowData{
		{
			TestName: "Disordered Columns",
			RowPtr:   4,
			Tab:      testT,
			Cols:     []string{"col2", "col1", "col4", "col3"},
			Vals:     []sqtypes.Raw{"test Five2", 5, "test Five4", 25},
			ExpVals:  []sqtypes.Raw{5, "test Five2", 25, "test Five4"},
			ExpErr:   "",
		},
		{
			TestName: "Null in Null Columns",
			RowPtr:   4,
			Tab:      testT,
			Cols:     []string{"col1", "col2", "col3", "col4"},
			Vals:     []sqtypes.Raw{5, nil, 25, "test Five4"},
			ExpVals:  []sqtypes.Raw{5, nil, 25, "test Five4"},
			ExpErr:   "",
		},
		{
			TestName: "Null in Not Null Columns",
			RowPtr:   4,
			Tab:      testT,
			Cols:     []string{"col1", "col2", "col3", "col4"},
			Vals:     []sqtypes.Raw{nil, nil, 25, "test Five4"},
			ExpVals:  []sqtypes.Raw{nil, nil, 25, "test Five4"},
			ExpErr:   "Error: Column \"col1\" in Table \"createrowtest\" can not be NULL",
		},
		{
			TestName: "To many Columns",
			RowPtr:   4,
			Tab:      testT,
			Cols:     []string{"col1", "col2", "col3", "col4", "col5"},
			Vals:     []sqtypes.Raw{5, "test Five2", 25, "test Five4", nil},
			ExpVals:  []sqtypes.Raw{5, "test Five2", 25, "test Five4", nil},
			ExpErr:   "Error: More columns are being set than exist in table definition",
		},
		{
			TestName: "Unknown Columns",
			RowPtr:   4,
			Tab:      testT,
			Cols:     []string{"col1", "col2", "col3", "col5"},
			Vals:     []sqtypes.Raw{5, "test Five2", 25, nil},
			ExpVals:  []sqtypes.Raw{5, "test Five2", 25, nil},
			ExpErr:   "Error: Column (col5) does not exist in table (createrowtest)",
		},
		{
			TestName: "Only Not Null Columns",
			RowPtr:   4,
			Tab:      testT,
			Cols:     []string{"col1", "col4"},
			Vals:     []sqtypes.Raw{5, "test Five4"},
			ExpVals:  []sqtypes.Raw{5, nil, nil, "test Five4"},
			ExpErr:   "",
		},
		{
			TestName: "Type Mismatch",
			RowPtr:   4,
			Tab:      testT,
			Cols:     []string{"col1", "col2", "col3", "col4"},
			Vals:     []sqtypes.Raw{true, nil, 25, "test Five4"},
			ExpVals:  []sqtypes.Raw{5, nil, 25, "test Five4"},
			ExpErr:   "Error: Type Mismatch: Column col1 in Table createrowtest has a type of INT, Unable to set value of type BOOL",
		},
		{
			TestName: "No Value for Not Null Columns",
			RowPtr:   4,
			Tab:      testT,
			Cols:     []string{"col2", "col3", "col4"},
			Vals:     []sqtypes.Raw{nil, 25, "test Five4"},
			ExpVals:  []sqtypes.Raw{nil, 25, "test Five4"},
			ExpErr:   "Error: Column \"col1\" in Table \"createrowtest\" can not be NULL",
		},
	}

	for _, rw := range testData {
		t.Run(rw.TestName, testCreateRowFunc(profile, &rw))
	}
}

type ColData struct {
	testName string
	row      *sqtables.RowDef
	col      sqtables.ColDef
	ExpVal   sqtypes.Raw
	ExpErr   string
}

func testGetColDataFunc(profile *sqprofile.SQProfile, r *ColData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		ExpVal := sqtypes.RawValue(r.ExpVal)
		val, err := r.row.GetColData(profile, &r.col)
		if sqtest.CheckErr(t, err, r.ExpErr) {
			return
		}

		if val.IsNull() && ExpVal.IsNull() {
			// they match so no error
			return
		}

		if !val.Equal(ExpVal) {
			t.Errorf("Returned value %q does not match expected %q", val.String(), ExpVal.String())
			return
		}

	}
}

func TestGetColData(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	// Setup Data
	tableName := "getcoldatatest"
	testT := sqtables.CreateTableDef(tableName,
		sqtables.NewColDef("col1", tokens.Int, true),
		sqtables.NewColDef("col2", tokens.String, false),
		sqtables.NewColDef("col3", tokens.Int, false),
		sqtables.NewColDef("col4", tokens.Bool, true),
	)
	err := sqtables.CreateTable(profile, testT)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	row1, err := sqtables.CreateRow(profile, 0, testT, testT.GetColNames(profile), sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{5, "Test Data 0", nil, true}))
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	rowD, err := sqtables.CreateRow(profile, 0, testT, testT.GetColNames(profile), sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{5, "Test Data 0", nil, true}))
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}
	rowD.Delete(profile)

	testData := []ColData{
		{"Get Int ColData", row1, sqtables.NewColDef("col1", tokens.Int, false), 5, ""},
		{"Get String ColData", row1, sqtables.NewColDef("col2", tokens.String, false), "Test Data 0", ""},
		{"Get Bool ColData", row1, sqtables.NewColDef("col4", tokens.Bool, false), true, ""},
		{"Get Null ColData", row1, sqtables.NewColDef("col3", tokens.Int, false), nil, ""},
		{"Type MisMatch ColData", row1, sqtables.NewColDef("col3", tokens.Null, false), nil, "Error: col3's type of NULL does not match table definition for table getcoldatatest"},
		{"Get Deleted Row ColData", rowD, sqtables.NewColDef("col1", tokens.Int, false), 5, "Error: Referenced Row has been deleted"},
		{
			testName: "Invalid Col",
			row:      row1,
			col:      sqtables.NewColDef("colX", tokens.Int, false),
			ExpVal:   314,
			ExpErr:   "Error: colX not found in table getcoldatatest",
		},
	}

	for _, rw := range testData {
		t.Run(rw.testName, testGetColDataFunc(profile, &rw))
	}
}
func TestSetStorage(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	profile := sqprofile.CreateSQProfile()

	// Setup Data
	tableName := "setstorage"
	testT := sqtables.CreateTableDef(tableName,
		sqtables.NewColDef("col1", tokens.Int, true),
		sqtables.NewColDef("col2", tokens.String, false),
		sqtables.NewColDef("col3", tokens.Int, false),
		sqtables.NewColDef("col4", tokens.Bool, true),
	)
	err := sqtables.CreateTable(profile, testT)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	row1, err := sqtables.CreateRow(profile, 0, testT, testT.GetColNames(profile), sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{5, "Test Data 0", nil, true}))
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}
	row1.SetStorage(profile, 1024, 4096, 256)

}

func TestMiscRowFunctions(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	// Setup Data
	tableName := "miscrowtest"
	testT := sqtables.CreateTableDef(tableName,
		sqtables.NewColDef("col1", tokens.Int, true),
		sqtables.NewColDef("col2", tokens.String, false),
		sqtables.NewColDef("col3", tokens.Int, false),
		sqtables.NewColDef("col4", tokens.String, true),
	)
	err := sqtables.CreateTable(profile, testT)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	tables := sqtables.NewTableListFromTableDef(profile, testT)
	dsData, err := sqtables.NewDataSet(profile, tables, sqtables.ColsToExpr(testT.GetCols(profile)), nil)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}
	dsData.Vals = sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
		{1, "test one2", 21, "test one4"},
		{2, "test two2", 22, "test two4"},
		{3, "test three2", 23, "test three4"},
		{4, "test four2", 24, "test four4"},
	})
	_, err = testT.AddRows(profile, dsData)
	if err != nil {
		t.Error("Error setting up table: ", err)
		return
	}

	ptr12 := sqptr.SQPtr(12)
	row1, err := sqtables.CreateRow(profile, ptr12, testT, testT.GetColNames(profile), sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{5, "Test Data 0", nil, "Original"}))
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}
	rowD, err := sqtables.CreateRow(profile, 0, testT, testT.GetColNames(profile), sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{6, "Test Data 0", nil, "Originald"}))
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}
	rowD.Delete(profile)

	t.Run("Row is valid RowInterface", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		var i sqtables.RowInterface
		i = row1
		_, ok := i.(sqtables.RowInterface)
		if !ok {
			t.Error("Row1 is not a RowInterface")
			return
		}
	})

	t.Run("GetPtr", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if row1.GetPtr(profile) != ptr12 {
			t.Error("GetPtr did not match expected value")
			return
		}
	})
	t.Run("GetIdxVal idx=-1", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		errTxt := "Error: Invalid index (-1) for row. Data len = 4"
		_, err := row1.GetIdxVal(profile, -1)

		if err.Error() != errTxt {
			t.Errorf("Expected err %q did not match actual error %q", errTxt, err)
			return
		}
	})
	t.Run("GetIdxVal idx=4", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		errTxt := "Error: Invalid index (4) for row. Data len = 4"
		_, err := row1.GetIdxVal(profile, 4)

		if err.Error() != errTxt {
			t.Errorf("Expected err %q did not match actual error %q", errTxt, err)
			return
		}
	})
	t.Run("GetIdxVal idx=1", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		errTxt := ""
		expVal := sqtypes.NewSQString("Test Data 0")
		v, err := row1.GetIdxVal(profile, 1)

		if err != nil && err.Error() != errTxt {
			t.Errorf("Expected err %q did not match actual error %q", errTxt, err)
			return
		}
		if !v.Equal(expVal) {
			t.Errorf("Expected Value %s does not match actual value %s", expVal.String(), v.String())
		}
	})
	t.Run("GetIdxVal deleted row", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		errTxt := "Internal Error: Deleted row can't return a value from GetIdxVal. Table: miscrowtest, ptr:0"
		expVal := sqtypes.NewSQString("Test Data 0")
		v, err := rowD.GetIdxVal(profile, 1)

		if err != nil {
			if err.Error() != errTxt {
				t.Errorf("Expected err %q did not match actual error %q", errTxt, err)
				return
			}
			return
		}
		if !v.Equal(expVal) {
			t.Errorf("Expected Value %s does not match actual value %s", expVal.String(), v.String())
		}
	})

}
