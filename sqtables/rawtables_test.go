package sqtables_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/wilphi/sqsrv/sqprofile"
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

//////////////////////////////////////////////

type CreateTableFromRawData struct {
	TestName  string
	TableName string
	RawData   sqtypes.RawVals
	ExpErr    string
	ExpVals   sqtypes.RawVals
	ExpCols   []column.Ref
}

func testCreateTableFromRawFunc(d CreateTableFromRawData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()
		tref, err := sqtables.CreateTableFromRaw(profile, d.TableName, d.RawData)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		tab, err := sqtables.GetTableRef(profile, d.TableName)
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(tref, tab) {
			t.Error("Returned vs Fetched tableref do not match")
		}
		clist := tab.Table.GetCols(profile)
		if clist.Len() != len(d.ExpCols) {
			t.Errorf("Actual Collist len (%d) does not match Expected (%d)", clist.Len(), len(d.ExpCols))
			return
		}
		for i, col := range clist.GetRefs() {
			if !reflect.DeepEqual(col, d.ExpCols[i]) {
				t.Errorf("Cols Not Equal - Actual: %v - Expected: %v", col, d.ExpCols[i])
				return
			}
			//fmt.Printf("%s, ", col.String())
		}
		ds, err := tab.GetRowData(profile, sqtables.ColsToExpr(clist), nil)
		if err != nil {
			t.Error(err)
			return
		}

		expVals := sqtypes.CreateValuesFromRaw(d.ExpVals)
		msg := sqtypes.Compare2DValue(ds.Vals, expVals, "Actual", "Expect", true)
		if msg != "" {
			t.Error(msg)
			return
		}
	}
}

func TestCreateTableFromRaw(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	data := []CreateTableFromRawData{
		{
			TestName:  "Basic Table",
			TableName: "cr_country",
			RawData: sqtypes.RawVals{
				{"name", "short"},
				{"Canada", "CAN"},
				{"United Kingdom", "GBR"},
				{"United States", "USA"},
			},
			ExpVals: sqtypes.RawVals{
				{"Canada", "CAN"},
				{"United Kingdom", "GBR"},
				{"United States", "USA"},
			},
			ExpCols: []column.Ref{
				{
					ColName:          "name",
					ColType:          tokens.String,
					Idx:              0,
					IsNotNull:        false,
					TableName:        moniker.New("cr_country", ""),
					DisplayTableName: false,
				},
				{
					ColName:          "short",
					ColType:          tokens.String,
					Idx:              1,
					IsNotNull:        false,
					TableName:        moniker.New("cr_country", ""),
					DisplayTableName: false,
				},
			},
		},
		{
			TestName:  "Existing Table",
			TableName: "cr_country",
			RawData: sqtypes.RawVals{
				{"name", "short"},
				{"Canada", "CAN"},
				{"United Kingdom", "GBR"},
				{"United States", "USA"},
			},
			ExpErr:  "Error: Invalid Name: Table cr_country already exists",
			ExpVals: sqtypes.RawVals{},
			ExpCols: []column.Ref{},
		},
		{
			TestName:  "Empty Table",
			TableName: "cr_empty",
			RawData: sqtypes.RawVals{
				{"name", "short"},
			},
			ExpErr:  "Error: No data to create table, must include a col row and atleast one data row",
			ExpVals: sqtypes.RawVals{},
			ExpCols: []column.Ref{},
		},
		{
			TestName:  "irregular Table",
			TableName: "cr_odd",
			RawData: sqtypes.RawVals{
				{"name", "short"},
				{1, 2},
				{3, 4, 5},
				{6, 7},
			},
			ExpErr:  "Internal Error: Row #1 has 3 values, it should have 2",
			ExpVals: sqtypes.RawVals{},
			ExpCols: []column.Ref{},
		},
		{
			TestName:  "null vals in Table",
			TableName: "cr_nulls",
			RawData: sqtypes.RawVals{
				{"name", "short"},
				{nil, "CAN"},
				{3, nil},
				{6, "TAR"},
			},
			ExpErr: "",
			ExpVals: sqtypes.RawVals{
				{nil, "CAN"},
				{3, nil},
				{6, "TAR"},
			},
			ExpCols: []column.Ref{
				{
					ColName:          "name",
					ColType:          tokens.Int,
					Idx:              0,
					IsNotNull:        false,
					TableName:        moniker.New("cr_nulls", ""),
					DisplayTableName: false,
				},
				{
					ColName:          "short",
					ColType:          tokens.String,
					Idx:              1,
					IsNotNull:        false,
					TableName:        moniker.New("cr_nulls", ""),
					DisplayTableName: false,
				},
			},
		},
		{
			TestName:  "Type Mismatch",
			TableName: "cr_mismatch",
			RawData: sqtypes.RawVals{
				{"name", "short"},
				{nil, "CAN"},
				{3, nil},
				{"6", "TAR"},
			},
			ExpErr:  "Internal Error: Value[2][0] Type (STRING) does not match the ColType of INT",
			ExpVals: sqtypes.RawVals{},
			ExpCols: []column.Ref{},
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testCreateTableFromRawFunc(row))

	}

}

////////////////////////////////////////////////////////////////////////////////////////
type ReadRawFromFileData struct {
	TestName string
	PathName string
	ExpErr   string
	ExpVals  sqtypes.RawVals
}

func testReadRawFromFileFunc(d ReadRawFromFileData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		rawVal, err := sqtables.ReadRawFromFile(d.PathName)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		msg := sqtypes.Compare2DRaw(rawVal, d.ExpVals, "Actual", "Expect")
		if msg != "" {
			t.Error(msg)
			return
		}
	}
}

func TestReadRawFromFile(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	data := []ReadRawFromFileData{
		{
			TestName: "Basic File",
			PathName: "./testdata/rawtables/basic.txt",
			ExpVals: sqtypes.RawVals{
				{"name", "short"},
				{"Canada", "CAN"},
				{"United Kingdom", "GBR"},
				{"United States", "USA"},
			},
		},
		{
			TestName: "ExtraLine",
			PathName: "./testdata/rawtables/extraline.txt",
			ExpVals:  sqtypes.RawVals{},
			ExpErr:   "Error: Source file: ./testdata/rawtables/extraline.txt can't contain blank lines",
		},
		{
			TestName: "Normal File",
			PathName: "./testdata/rawtables/normal.txt",
			ExpVals: sqtypes.RawVals{
				{"col1", "col2", "col3", "col4", "col5"},
				{1, "test 1", true, 1.75, -1},
				{2, "test 2", false, -3.75, -2},
				{3, nil, true, 7.87632738929, -3},
			},
		},
		{
			TestName: "Ident File",
			PathName: "./testdata/rawtables/ident.txt",
			ExpErr:   "Error: Unexpected Token [IDENT=test] in line 1",
			ExpVals:  sqtypes.RawVals{},
		},
		{
			TestName: "minus File",
			PathName: "./testdata/rawtables/minus.txt",
			ExpErr:   "Error: Unexpected Minus sign in line 2",
			ExpVals:  sqtypes.RawVals{},
		},
		{
			TestName: "select File",
			PathName: "./testdata/rawtables/select.txt",
			ExpErr:   "Error: Unexpected Token SELECT in line 1",
			ExpVals:  sqtypes.RawVals{},
		},
	}
	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testReadRawFromFileFunc(row))

	}

}
