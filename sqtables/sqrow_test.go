package sqtables_test

import (
	"fmt"
	"log"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

type CreateRowData struct {
	testName string
	RowNum   int64
	Tab      *sqtables.TableDef
	Cols     []string
	Vals     []sqtypes.Value
	ExpVals  []sqtypes.Value
	ExpErr   string
}

func testCreateRowFunc(profile *sqprofile.SQProfile, r *CreateRowData) func(*testing.T) {
	return func(t *testing.T) {
		row, err := sqtables.CreateRow(profile, r.RowNum, r.Tab, r.Cols, r.Vals)
		if err != nil {
			log.Println(err.Error())
			if r.ExpErr == "" {
				t.Errorf("Unexpected Error in test: %s", err.Error())
				return
			}
			if r.ExpErr != err.Error() {
				t.Errorf("Expecting Error %s but got: %s", r.ExpErr, err.Error())
				return
			}
			return
		}
		if err == nil && r.ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", r.ExpErr)
			return
		}

		// make sure the data matches expected
		for i, val := range row.Data {

			if (val.IsNull() && !r.ExpVals[i].IsNull()) || (!val.IsNull() && r.ExpVals[i].IsNull()) {
				t.Errorf("Returned value %q does not match expected %q", val.ToString(), r.ExpVals[i].ToString())
				return
			}

			if !(val.IsNull() && r.ExpVals[i].IsNull()) {
				if !val.Equal(r.ExpVals[i]) {
					t.Errorf("Returned value %q does not match expected %q", val.ToString(), r.ExpVals[i].ToString())
					return
				}
			}
		}

	}
}

func TestCreateRow(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	// Setup Data
	stmt := "CREATE TABLE createrowtest (col1 int not null, col2 string null, col3 int, col4 string not null)"
	tkList := tokens.Tokenize(stmt)
	tableName, err := cmd.CreateTableFromTokens(profile, tkList)
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	testT := sqtables.GetTable(profile, tableName)
	//cols := testT.GetCols(false)
	stmt = "INSERT INTO " + tableName + "(col1, col2, col3, col4) VALUES "
	stmt += fmt.Sprintf("(%d, %q, %d, %q), ", 1, "test one2", 21, "test one4")
	stmt += fmt.Sprintf("(%d, %q, %d, %q), ", 2, "test two2", 22, "test two4")
	stmt += fmt.Sprintf("(%d, %q, %d, %q), ", 3, "test three2", 23, "test three4")
	stmt += fmt.Sprintf("(%d, %q, %d, %q) ", 4, "test four2", 24, "test four4")
	tkList = tokens.Tokenize(stmt)
	_, _, err = cmd.InsertInto(profile, tkList)
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	testData := []CreateRowData{
		{"Disordered Columns", 4, testT, []string{"col2", "col1", "col4", "col3"},
			[]sqtypes.Value{sqtypes.NewSQString("test Five2"), sqtypes.NewSQInt(5), sqtypes.NewSQString("test Five4"), sqtypes.NewSQInt(25)},
			[]sqtypes.Value{sqtypes.NewSQInt(5), sqtypes.NewSQString("test Five2"), sqtypes.NewSQInt(25), sqtypes.NewSQString("test Five4")},
			"",
		},
		{"Null in Null Columns", 4, testT, []string{"col1", "col2", "col3", "col4"},
			[]sqtypes.Value{sqtypes.NewSQInt(5), sqtypes.NewSQNull(), sqtypes.NewSQInt(25), sqtypes.NewSQString("test Five4")},
			[]sqtypes.Value{sqtypes.NewSQInt(5), sqtypes.NewSQNull(), sqtypes.NewSQInt(25), sqtypes.NewSQString("test Five4")},
			"",
		},
		{"Null in Not Null Columns", 4, testT, []string{"col1", "col2", "col3", "col4"},
			[]sqtypes.Value{sqtypes.NewSQNull(), sqtypes.NewSQNull(), sqtypes.NewSQInt(25), sqtypes.NewSQString("test Five4")},
			[]sqtypes.Value{sqtypes.NewSQNull(), sqtypes.NewSQNull(), sqtypes.NewSQInt(25), sqtypes.NewSQString("test Five4")},
			"Error: Column \"col1\" in Table \"createrowtest\" can not be NULL",
		},
		{"To many Columns", 4, testT, []string{"col1", "col2", "col3", "col4", "col5"},
			[]sqtypes.Value{sqtypes.NewSQInt(5), sqtypes.NewSQString("test Five2"), sqtypes.NewSQInt(25), sqtypes.NewSQString("test Five4"), sqtypes.NewSQNull()},
			[]sqtypes.Value{sqtypes.NewSQInt(5), sqtypes.NewSQString("test Five2"), sqtypes.NewSQInt(25), sqtypes.NewSQString("test Five4"), sqtypes.NewSQNull()},
			"Error: More columns are being set than exist in table definition",
		},
		{"Unknown Columns", 4, testT, []string{"col1", "col2", "col3", "col5"},
			[]sqtypes.Value{sqtypes.NewSQInt(5), sqtypes.NewSQString("test Five2"), sqtypes.NewSQInt(25), sqtypes.NewSQNull()},
			[]sqtypes.Value{sqtypes.NewSQInt(5), sqtypes.NewSQString("test Five2"), sqtypes.NewSQInt(25), sqtypes.NewSQNull()},
			"Error: Column (col5) does not exist in table (createrowtest)",
		},
		{"Only Not Null Columns", 4, testT, []string{"col1", "col4"},
			[]sqtypes.Value{sqtypes.NewSQInt(5), sqtypes.NewSQString("test Five4")},
			[]sqtypes.Value{sqtypes.NewSQInt(5), sqtypes.NewSQNull(), sqtypes.NewSQNull(), sqtypes.NewSQString("test Five4")},
			"",
		},
	}

	for _, rw := range testData {
		t.Run(rw.testName, testCreateRowFunc(profile, &rw))
	}
}

type ColData struct {
	testName string
	row      *sqtables.RowDef
	col      sqtables.ColDef
	ExpVal   sqtypes.Value
	ExpErr   string
}

func testGetColDataFunc(profile *sqprofile.SQProfile, r *ColData) func(*testing.T) {
	return func(t *testing.T) {
		val, err := r.row.GetColData(profile, &r.col)
		if err != nil {
			log.Println(err.Error())
			if r.ExpErr == "" {
				t.Errorf("Unexpected Error in test: %s", err.Error())
				return
			}
			if r.ExpErr != err.Error() {
				t.Errorf("Expecting Error %s but got: %s", r.ExpErr, err.Error())
				return
			}
			return
		}
		if err == nil && r.ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", r.ExpErr)
			return
		}

		if val.IsNull() && r.ExpVal.IsNull() {
			// they match so no error
			return
		}

		if !val.Equal(r.ExpVal) {
			t.Errorf("Returned value %q does not match expected %q", val.ToString(), r.ExpVal.ToString())
			return
		}

	}
}

func TestGetColData(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	// Setup Data
	stmt := "CREATE TABLE getcoldatatest (col1 int not null, col2 string null, col3 int, col4 bool not null)"
	tkList := tokens.Tokenize(stmt)
	tableName, err := cmd.CreateTableFromTokens(profile, tkList)
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	testT := sqtables.GetTable(profile, tableName)

	row1, err := sqtables.CreateRow(profile, 0, testT, testT.GetColNames(profile), []sqtypes.Value{sqtypes.NewSQInt(5), sqtypes.NewSQString("Test Data 0"), sqtypes.NewSQNull(), sqtypes.NewSQBool(true)})
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}
	/*
		rowD, err := sqtables.CreateRow(profile, 0, testT, testT.GetColNames(profile), []sqtypes.Value{sqtypes.NewSQInt(5), sqtypes.NewSQString("Test Data 0"), sqtypes.NewSQNull(), sqtypes.NewSQBool(true)})
		if err != nil {
			t.Fatalf("Unexpected Error setting up test: %s", err.Error())
		}
		//rowD.Delete()
	*/

	testData := []ColData{
		{"Get Int ColData", row1, sqtables.CreateColDef("col1", tokens.TypeInt, false), sqtypes.NewSQInt(5), ""},
		{"Get String ColData", row1, sqtables.CreateColDef("col2", tokens.TypeString, false), sqtypes.NewSQString("Test Data 0"), ""},
		{"Get Bool ColData", row1, sqtables.CreateColDef("col4", tokens.TypeBool, false), sqtypes.NewSQBool(true), ""},
		{"Get Null ColData", row1, sqtables.CreateColDef("col3", tokens.TypeInt, false), sqtypes.NewSQNull(), ""},
		{"Type MisMatch ColData", row1, sqtables.CreateColDef("col3", tokens.Null, false), sqtypes.NewSQNull(), "Error: col3's type of NULL does not match table definition for table getcoldatatest"},
		//		{"Get Deleted Row ColData", rowD, sqtables.CreateColDef("col1", tokens.TypeInt, false), sqtypes.NewSQInt(5), "Error: Referenced Row has been deleted"},
	}

	for _, rw := range testData {
		t.Run(rw.testName, testGetColDataFunc(profile, &rw))
	}
}

/*
type SetRowData struct {
	testName  string
	row       *sqtables.RowDef
	colNames  []string
	colValues []sqtypes.Value
	ExpVals   []sqtypes.Value
	ExpErr    string
}

func testSetRowFunc(r *SetRowData) func(*testing.T) {
	return func(t *testing.T) {
		err := r.row.SetRow(r.colNames, r.colValues)
		if err != nil {
			log.Println(err.Error())
			if r.ExpErr == "" {
				t.Errorf("Unexpected Error in test: %s", err.Error())
				return
			}
			if r.ExpErr != err.Error() {
				t.Errorf("Expecting Error %s but got: %s", r.ExpErr, err.Error())
				return
			}
			return
		}
		if err == nil && r.ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", r.ExpErr)
			return
		}

		// make sure the data matches expected
		for i, val := range r.row.Data {

			if (val.IsNull() && !r.ExpVals[i].IsNull()) || (!val.IsNull() && r.ExpVals[i].IsNull()) {
				t.Errorf("Returned value %q does not match expected %q", val.ToString(), r.ExpVals[i].ToString())
				return
			}

			if !(val.IsNull() && r.ExpVals[i].IsNull()) {
				if !val.Equal(r.ExpVals[i]) {
					t.Errorf("Returned value %q does not match expected %q", val.ToString(), r.ExpVals[i].ToString())
					return
				}
			}
		}

	}
}

func TestSetRowData(t *testing.T) {
	// Setup Data
	stmt := "CREATE TABLE getcoldatatest (col1 int not null, col2 string null, col3 int, col4 bool not null)"
	tkList := tokens.Tokenize(stmt)
	tableName, err := cmd.CreateTableFromTokens(*tkList)
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}

	testT := sqtables.GetTable(tableName)

	row1, err := sqtables.CreateRow(0, testT, testT.GetColNames(), []sqtypes.Value{sqtypes.NewSQInt(5), sqtypes.NewSQString("Test Data 0"), sqtypes.NewSQNull(), sqtypes.NewSQBool(true)})
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}
	rowD, err := sqtables.CreateRow(0, testT, testT.GetColNames(), []sqtypes.Value{sqtypes.NewSQInt(5), sqtypes.NewSQString("Test Data 0"), sqtypes.NewSQNull(), sqtypes.NewSQBool(true)})
	if err != nil {
		t.Fatalf("Unexpected Error setting up test: %s", err.Error())
	}
	rowD.Delete()

	testData := []ColData{
		{"Get Int ColData", row1, sqtables.CreateColDef("col1", tokens.TypeInt, false), sqtypes.NewSQInt(5), ""},
		{"Get String ColData", row1, sqtables.CreateColDef("col2", tokens.TypeString, false), sqtypes.NewSQString("Test Data 0"), ""},
		{"Get Bool ColData", row1, sqtables.CreateColDef("col4", tokens.TypeBool, false), sqtypes.NewSQBool(true), ""},
		{"Get Null ColData", row1, sqtables.CreateColDef("col3", tokens.TypeInt, false), sqtypes.NewSQNull(), ""},
		{"Type MisMatch ColData", row1, sqtables.CreateColDef("col3", tokens.Null, false), sqtypes.NewSQNull(), "Error: col3's type of NULL does not match table definition for table getcoldatatest"},
		{"Get Deleted Row ColData", rowD, sqtables.CreateColDef("col1", tokens.TypeInt, false), sqtypes.NewSQInt(5), "Error: Referenced Row has been deleted"},
	}

	for _, rw := range testData {
		t.Run(rw.testName, testGetColDataFunc(&rw))
	}
}
*/
