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

func TestRef(t *testing.T) {

	tlist1 := moniker.New("tlist1", "")

	data := []RefData{
		{
			TestName:  "NewRef with nulls",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: false,
			ExpString: "{col1, INT}",
			ExpName:   "col1",
			TableName: tlist1,
		},
		{
			TestName:  "NewRef without nulls",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: true,
			ExpString: "{col1, INT NOT NULL}",
			ExpName:   "col1",
			TableName: tlist1,
		},
		{
			TestName:         "NewRef all values",
			ColName:          "col1",
			ColType:          tokens.Int,
			IsNotNull:        true,
			Idx:              5,
			TableName:        moniker.New("testTab", ""),
			DisplayTableName: true,
			ExpString:        "{testtab.col1, INT NOT NULL}",
			ExpName:          "testtab.col1",
		},
		{
			TestName:         "NewRef do not display tablename",
			ColName:          "col1",
			ColType:          tokens.Int,
			IsNotNull:        true,
			Idx:              5,
			TableName:        moniker.New("testTab", ""),
			DisplayTableName: false,
			ExpString:        "{col1, INT NOT NULL}",
			ExpName:          "col1",
		},
		{
			TestName:  "NewRef Merge",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: true,
			MergeCD:   &column.Def{ColName: "col1", ColType: tokens.Int, Idx: 12, IsNotNull: false, TableName: "tlist1"},
			ExpCD:     column.Ref{ColName: "col1", ColType: tokens.Int, Idx: 12, IsNotNull: false, TableName: moniker.New("tlist1", ""), DisplayTableName: false},
			ExpString: "{col1, INT NOT NULL}",
			ExpName:   "col1",
			TableName: tlist1,
		},
		{
			TestName:         "NewRef Merge with TableAlias",
			ColName:          "col1",
			ColType:          tokens.Int,
			IsNotNull:        true,
			TableName:        moniker.New("testTab", "alias1"),
			DisplayTableName: true,
			MergeCD:          &column.Def{ColName: "col1", ColType: tokens.Int, Idx: 12, IsNotNull: false, TableName: "testtab"},
			ExpCD:            column.Ref{ColName: "col1", ColType: tokens.Int, Idx: 12, IsNotNull: false, TableName: moniker.New("testTab", "alias1"), DisplayTableName: true},
			ExpString:        "{alias1.col1, INT NOT NULL}",
			ExpName:          "alias1.col1",
		},
		{
			TestName:  "NewRef Merge Error",
			ColName:   "col1",
			ColType:   tokens.Int,
			IsNotNull: true,
			MergeCD:   &column.Def{ColName: "col2", ColType: tokens.Int, Idx: 12, IsNotNull: false, TableName: "tlist1"},
			ExpCD:     column.Ref{ColName: "col1", ColType: tokens.Int, Idx: 12, IsNotNull: false, TableName: moniker.New("tlist1", ""), DisplayTableName: false},
			ExpString: "{col1, INT NOT NULL}",
			ExpErr:    "Internal Error: Can't merge Ref col1, col2",
			ExpName:   "col1",
			TableName: tlist1,
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testRefFunc(row))

	}

}

type RefData struct {
	TestName         string
	ColName          string
	ColType          tokens.TokenID
	TableName        *moniker.Moniker
	Idx              int
	IsNotNull        bool
	DisplayTableName bool
	MergeCD          *column.Def
	ExpCD            column.Ref
	ExpString        string
	ExpErr           string
	ExpName          string
}

func testRefFunc(d RefData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		cd := column.NewRef(d.ColName, d.ColType, d.IsNotNull)
		if d.ColName != cd.ColName || d.ColType != cd.ColType || cd.Idx != -1 || d.IsNotNull != cd.IsNotNull {
			t.Errorf("Created column.Ref does not match expected")
			return
		}
		if d.Idx != 0 {
			cd.Idx = d.Idx
		}
		cd.TableName = d.TableName.Clone()
		cd.DisplayTableName = d.DisplayTableName
		if d.ExpString != cd.String() {
			t.Errorf("String %q does not match expected: %q", cd.String(), d.ExpString)

		}

		if d.ExpName != cd.DisplayName() {
			t.Errorf("DisplayName %q does not match expected: %q", cd.DisplayName(), d.ExpName)

		}

		if cd.TableName.Show() != cd.GetTableName() {
			t.Errorf("GetTableName %q does not match expected: %q", cd.GetTableName(), cd.TableName.Show())

		}

		bin := sqbin.NewCodec(nil)
		cd.Encode(bin)
		newCd := column.Ref{}
		newCd.Decode(bin)

		//fmt.Printf(" Original: %v\nRecreated: %v\n", cd, newCd)
		if !reflect.DeepEqual(cd, newCd) {
			t.Error("column.Ref encoded/decoded does not match original")
		}
		if d.MergeCD != nil {
			newCD, err := column.MergeRefDef(cd, *d.MergeCD)
			if sqtest.CheckErr(t, err, d.ExpErr) {
				return
			}

			if !reflect.DeepEqual(newCD, d.ExpCD) {
				t.Errorf("column.Ref Merge: Expected %v does not match actual %v", d.ExpCD, newCD)
			}

		}

	}
}
