package column_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("column_test.log")
}

func TestDef(t *testing.T) {
	data := []DefData{
		{
			TestName:  "NewDef with nulls",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: false,
			ExpString: "{col1, INT}",
		},
		{
			TestName:  "NewDef without nulls",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: true,
			ExpString: "{col1, INT NOT NULL}",
		},
		{
			TestName:  "NewDef all values",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: true,
			Idx:       5,
			TableName: "testTab",
			ExpString: "{col1, INT NOT NULL}",
		},
		{
			TestName:  "NewDef do not display tablename",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: true,
			Idx:       5,
			TableName: "testTab",
			ExpString: "{col1, INT NOT NULL}",
		},
		{
			TestName:  "NewDef Merge",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: true,
			ExpCD:     column.Def{ColName: "col1", ColType: tokens.Int, Idx: 12, IsNotNull: false, TableName: "tlist1"},
			ExpString: "{col1, INT NOT NULL}",
		},
		{
			TestName:  "NewDef Merge Error",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: true,
			ExpCD:     column.Def{ColName: "col1", ColType: tokens.Int, Idx: 12, IsNotNull: false, TableName: "tlist1"},
			ExpString: "{col1, INT NOT NULL}",
			ExpErr:    "Internal Error: Can't merge Def col1, col2",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testDefFunc(row))

	}

}

type DefData struct {
	TestName  string
	ColName   string
	ColType   tokens.TokenID
	TableName string
	Idx       int
	IsNotNull bool
	ExpCD     column.Def
	ExpString string
	ExpErr    string
}

func testDefFunc(d DefData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		cd := column.NewDef(d.ColName, d.ColType, d.IsNotNull)
		if d.ColName != cd.ColName || d.ColType != cd.ColType || cd.Idx != -1 || d.IsNotNull != cd.IsNotNull {
			t.Errorf("Created column.Def does not match expected")
			return
		}
		if d.Idx != 0 {
			cd.Idx = d.Idx
		}
		if d.TableName != "" {
			cd.TableName = d.TableName
		}

		if d.ExpString != cd.String() {
			t.Errorf("String %q does not match expected: %q", cd.String(), d.ExpString)

		}

		colref := cd.Ref()
		if colref.ColName != cd.ColName || colref.ColType != cd.ColType || colref.Idx != cd.Idx || colref.IsNotNull != cd.IsNotNull ||
			!moniker.Equal(colref.TableName, moniker.New(cd.TableName, "")) || colref.DisplayTableName != false {
			t.Errorf(".Ref() value did not match expected: %v", colref)
		}
		bin := sqbin.NewCodec(nil)
		cd.Encode(bin)
		newCd := column.Def{}
		newCd.Decode(bin)

		//fmt.Printf(" Original: %v\nRecreated: %v\n", cd, newCd)
		if !reflect.DeepEqual(cd, newCd) {
			t.Error("column.Def encoded/decoded does not match original")
		}

		nextCd := cd.Clone()
		if !reflect.DeepEqual(cd, nextCd) {
			t.Error("column.Def.Clone() does not match original")
		}

	}
}
