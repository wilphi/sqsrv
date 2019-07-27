package sqtables_test

import (
	"fmt"
	"log"
	"reflect"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

func testNewExprDataSetFunc(tab *sqtables.TableDef, eList *sqtables.ExprList, tobeCreated bool) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()

		data := sqtables.NewExprDataSet(tab, eList)
		if tobeCreated {
			if data == nil {
				t.Error("Dataset not created")
				return
			}
		} else {
			if data != nil {
				t.Error("Dataset was created when it should not")
				return
			}
			return
		}

		colStr := eList.GetNames()
		c := data.GetColNames()
		if !reflect.DeepEqual(c, colStr) {
			t.Errorf("Column lists do not match: %v, %v", c, colStr)
			return
		}
		if data.Len() != 0 {
			t.Errorf("There should be no data in Dataset")

		}
	}
}

func testNewDataSetFunc(tab *sqtables.TableDef, cols sqtables.ColList, colStr []string, tobeCreated bool, ExpErr string) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		profile := sqprofile.CreateSQProfile()
		data, err := sqtables.NewDataSet(profile, tab, cols)
		if err != nil {
			log.Println(err.Error())
			if ExpErr == "" {
				t.Errorf("Unexpected Error in test: %s", err.Error())
				return
			}
			if ExpErr != err.Error() {
				t.Errorf("Expecting Error %s but got: %s", ExpErr, err.Error())
				return
			}
			return
		}

		if tobeCreated {
			if data == nil {
				t.Error("Dataset not created")
				return
			}
		} else {
			if data != nil {
				t.Error("Dataset was created when it should not")
				return
			}
			return
		}

		c := data.GetColNames()
		if !reflect.DeepEqual(c, colStr) {
			t.Errorf("Column lists do not match: %v, %v", c, colStr)
			return
		}
	}
}
func TestDataSet(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	str := "CREATE TABLE dataset (col1 int, col2 string, col3 bool)"
	tkns := tokens.Tokenize(str)
	tableName, err := cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		t.Error("Unable to create table for testing")
		return
	}
	str = "CREATE TABLE dataset2 (col1 int, col2 string, col3 bool)"
	tkns = tokens.Tokenize(str)
	tableName2, err := cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		t.Error("Unable to create table for testing")
		return
	}
	str = "INSERT INTO dataset (col1, col2, col3) VALUES " +
		fmt.Sprintf("(%d, %q, %v), ", 1, "ttt", true) +
		fmt.Sprintf("(%d, %q, %v), ", 9, "ttt", true) +
		fmt.Sprintf("(%d, %q, %v), ", 6, "aaa", true) +
		fmt.Sprintf("(%d, %q, %v), ", 6, "aaa", true) +
		fmt.Sprintf("(%d, %q, %v), ", 4, "qqq", false) +
		fmt.Sprintf("(%d, %q, %v), ", 2, "qab", true) +
		fmt.Sprintf("(%d, %q, %v), ", 8, "qxc", true) +
		fmt.Sprintf("(%d, %q, %v) ", 1, "nnn", true)
	_, _, err = cmd.InsertInto(profile, tokens.Tokenize(str))
	if err != nil {
		t.Errorf("Unable to insert data for testing: %s", err)
		return
	}

	colStr := []string{"col2", "col1"}
	colStrErr := []string{"col2", "col1", "colX"}

	colds := []sqtables.ColDef{sqtables.ColDef{ColName: "col2", ColType: "STRING"}, sqtables.ColDef{ColName: "col1", ColType: "INT"}}
	exprCols := sqtables.ColsToExpr(sqtables.NewColListDefs(colds))
	emptyExprCols := &sqtables.ExprList{}
	tab := sqtables.GetTable(profile, tableName)
	if tab == nil {
		t.Error("Unable to get table for testing")
		return
	}
	t.Run("New DataSet", testNewDataSetFunc(tab, sqtables.NewColListNames(colStr), colStr, true, ""))
	t.Run("New DataSet with Validate Err", testNewDataSetFunc(tab,
		sqtables.NewColListNames(colStrErr),
		colStr,
		true,
		"Error: Table dataset does not have a column named colX",
	))

	t.Run("New DataSet no Cols", testNewDataSetFunc(tab, sqtables.NewColListNames([]string{}), []string{}, false, ""))

	t.Run("New Expr DataSet", testNewExprDataSetFunc(tab, exprCols, true))

	t.Run("New Expr DataSet no Cols", testNewExprDataSetFunc(tab, emptyExprCols, false))

	t.Run("Len==0 from DataSet", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		data := sqtables.NewExprDataSet(tab, exprCols)
		if data == nil {
			t.Error("Dataset not created")
			return
		}

		if data.Len() != 0 {
			t.Error("There should be no data")
		}
	})

	t.Run("GetTable from Dataset", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		data := sqtables.NewExprDataSet(tab, exprCols)
		if data == nil {
			t.Error("Dataset not created")
			return
		}

		if tab.GetName(profile) != data.GetTable().GetName(profile) {
			t.Error("Tables do not match")
		}
	})
	t.Run("GetColList from Dataset", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(t.Name() + " panicked unexpectedly")
			}
		}()
		data := sqtables.NewExprDataSet(tab, exprCols)
		if data == nil {
			t.Error("Dataset not created")
			return
		}

		clist := data.GetColList()
		elist := exprCols.GetNames()
		if !reflect.DeepEqual(clist.GetColNames(), elist) {
			t.Error("Collist names do not match dataset")
		}
	})

	rw1 := []sqtypes.Value{sqtypes.NewSQString("aaa"), sqtypes.NewSQInt(6)}
	rw2 := []sqtypes.Value{sqtypes.NewSQString("nnn"), sqtypes.NewSQInt(1)}
	rw3 := []sqtypes.Value{sqtypes.NewSQString("qab"), sqtypes.NewSQInt(2)}
	rw4 := []sqtypes.Value{sqtypes.NewSQString("qqq"), sqtypes.NewSQInt(4)}
	rw5 := []sqtypes.Value{sqtypes.NewSQString("qxc"), sqtypes.NewSQInt(8)}
	rw6 := []sqtypes.Value{sqtypes.NewSQString("ttt"), sqtypes.NewSQInt(1)}
	rw7 := []sqtypes.Value{sqtypes.NewSQString("ttt"), sqtypes.NewSQInt(9)}

	data := []SortData{
		{
			TestName:        "Sort Dataset Invalid Order Col",
			TableName:       tableName,
			DataCols:        exprCols,
			Order:           []sqtables.OrderItem{{ColName: "colX", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			ExpVals:         [][]sqtypes.Value{rw1, rw1, rw2, rw3, rw4, rw5, rw6, rw7},
			ExpSortOrderErr: true,
		},
		{
			TestName:        "Sort Dataset skip Invalid Order Col",
			TableName:       tableName,
			DataCols:        exprCols,
			Order:           []sqtables.OrderItem{{ColName: "colX", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			ExpVals:         [][]sqtypes.Value{rw1, rw1, rw2, rw3, rw4, rw5, rw6, rw7},
			ExpSortOrderErr: true,
			ExpSortErr:      true,
		},
		{
			TestName:  "Sort Dataset",
			TableName: tableName,
			DataCols:  exprCols,
			Order:     []sqtables.OrderItem{{ColName: "col2", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			ExpVals:   [][]sqtypes.Value{rw1, rw1, rw2, rw3, rw4, rw5, rw6, rw7},
		},
		{
			TestName:  "Sort Empty Dataset",
			TableName: tableName2,
			DataCols:  exprCols,
			Order:     []sqtables.OrderItem{{ColName: "col2", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			ExpVals:   [][]sqtypes.Value{},
		},
		{
			TestName:  "Sort Dataset desc",
			TableName: tableName,
			DataCols:  exprCols,
			Order:     []sqtables.OrderItem{{ColName: "col2", SortType: tokens.Desc}, {ColName: "col1", SortType: tokens.Desc}},
			ExpVals:   [][]sqtypes.Value{rw7, rw6, rw5, rw4, rw3, rw2, rw1, rw1},
		},
		{
			TestName:  "Sort Dataset desc/asc",
			TableName: tableName,
			DataCols:  exprCols,
			Order:     []sqtables.OrderItem{{ColName: "col2", SortType: tokens.Desc}, {ColName: "col1", SortType: tokens.Asc}},
			ExpVals:   [][]sqtypes.Value{rw6, rw7, rw5, rw4, rw3, rw2, rw1, rw1},
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testSortFunc(row))

	}

}

type SortData struct {
	TestName        string
	TableName       string
	DataCols        *sqtables.ExprList
	Order           []sqtables.OrderItem
	ExpVals         [][]sqtypes.Value
	ExpSortOrderErr bool
	ExpSortErr      bool
}

func testSortFunc(d SortData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		profile := sqprofile.CreateSQProfile()

		tab := sqtables.GetTable(profile, d.TableName)
		if tab == nil {
			t.Errorf("Error getting table %s for sort", d.TableName)
			return
		}
		data, err := tab.GetRowData(profile, d.DataCols, nil)
		if err != nil {
			t.Errorf("Error getting data for sort: %s", err)
			return
		}
		err = data.SetOrder(d.Order)
		if !d.ExpSortErr && err != nil {
			if !d.ExpSortOrderErr {
				t.Errorf("Error using SetOrder")
			}
			return
		}
		err = data.Sort()
		if err != nil {
			if !d.ExpSortErr {
				t.Errorf("Error using Sort")
			}
			return
		}
		//fmt.Println(data.Vals)
		//fmt.Println(d.ExpVals)
		if !reflect.DeepEqual(data.Vals, d.ExpVals) {
			t.Error("The actual values after the Sort did not match expected values")
			return
		}

	}
}
