package sqtables_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("sqtables_test.log")
}

func testNewDataSetFunc(
	tables *sqtables.TableList,
	eList *sqtables.ExprList, groupBy *sqtables.ColList, ExpErr string,
) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()

		data, err := sqtables.NewDataSet(profile, tables, eList, nil)
		if sqtest.CheckErr(t, err, ExpErr) {
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

func TestDataSet(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	str := "CREATE TABLE dataset (col1 int, col2 string, col3 bool)"
	tkns := tokens.Tokenize(str)
	tableName, err := cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		t.Error("Unable to create table for testing")
		return
	}

	vals := sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
		{"ttt", 1},
		{"ttt", 9},
		{"aaa", 6},
		{"aaa", 6},
		{"qqq", 4},
		{"qab", 2},
		{"qxc", 8},
		{"nnn", 1},
	})
	valsWithNull := sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
		{nil, 11},
		{"ttt", 1},
		{"ttt", 9},
		{"aaa", 6},
		{"aaa", 6},
		{nil, 12},
		{"qqq", 4},
		{"qab", 2},
		{"qxc", 8},
		{"nnn", 1},
		{nil, 10},
	})
	//colStr := []string{"col2", "col1"}
	colStrErr := []string{"col2", "col1", "colX"}

	colds := []sqtables.ColDef{{ColName: "col2", ColType: tokens.String}, {ColName: "col1", ColType: tokens.Int}}
	exprCols := sqtables.ColsToExpr(sqtables.NewColListDefs(colds))
	emptyExprCols := &sqtables.ExprList{}
	exprColsErr := sqtables.ColsToExpr(sqtables.NewColListNames(colStrErr))
	tab1, err := sqtables.GetTable(profile, tableName)
	if err != nil {
		t.Error(err)
		return
	}

	if tab1 == nil {
		t.Error("Unable to get table for testing")
		return
	}
	tables := sqtables.NewTableListFromTableDef(profile, tab1)

	t.Run("New DataSet", testNewDataSetFunc(tables, exprCols, nil, ""))

	t.Run("New DataSet no Cols", testNewDataSetFunc(tables, emptyExprCols, nil, "Internal Error: Expression List is empty for new DataSet"))
	t.Run("New DataSet with Validate Err", testNewDataSetFunc(tables,
		exprColsErr,
		nil,
		"Error: Column \"colX\" not found in Table(s): dataset",
	))

	t.Run("Len==0 from DataSet", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		data, err := sqtables.NewDataSet(profile, tables, exprCols, nil)
		if err != nil {
			t.Errorf("Unexpected Error in test: %s", err.Error())
			return
		}

		if data.Len() != 0 {
			t.Error("There should be no data")
		}
	})

	t.Run("GetTable from Dataset", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		data, err := sqtables.NewDataSet(profile, tables, exprCols, nil)
		if err != nil {
			t.Errorf("Unexpected Error in test: %s", err.Error())
			return
		}

		if tables != data.GetTables() {
			t.Error("Tables do not match")
		}
	})
	t.Run("GetColList from Dataset", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		data, err := sqtables.NewDataSet(profile, tables, exprCols, nil)
		if err != nil {
			t.Errorf("Unexpected Error in test: %s", err.Error())
			return
		}
		clist := data.GetColList().GetColNames()

		nlist := data.GetColNames()
		elist := exprCols.GetNames()
		//	fmt.Printf("clist = %v\n", clist)
		//	fmt.Printf("elist = %v\n", elist)
		if !reflect.DeepEqual(nlist, elist) {
			t.Error("Collist names do not match dataset")
		}
		if !reflect.DeepEqual(clist, elist) {
			t.Error("GetColList does not match expected col list")
		}
	})

	rw1 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"aaa", 6})
	rw2 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"nnn", 1})
	rw3 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"qab", 2})
	rw4 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"qqq", 4})
	rw5 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"qxc", 8})
	rw6 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"ttt", 1})
	rw7 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{"ttt", 9})
	rwNil10 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{nil, 10})
	rwNil11 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{nil, 11})
	rwNil12 := sqtypes.CreateValueArrayFromRaw([]sqtypes.Raw{nil, 12})

	data := []SortData{
		{
			TestName:     "Sort Dataset Invalid Order Col",
			Tables:       tables,
			DataCols:     exprCols,
			InitVals:     vals,
			Order:        []sqtables.OrderItem{{ColName: "colX", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			ExpVals:      [][]sqtypes.Value{rw1, rw1, rw2, rw3, rw4, rw5, rw6, rw7},
			SortOrderErr: "Error: Column colX not found in dataset",
		},
		{
			TestName:     "Sort Dataset skip Invalid Order Col",
			Tables:       tables,
			DataCols:     exprCols,
			InitVals:     vals,
			Order:        []sqtables.OrderItem{{ColName: "colX", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			ExpVals:      [][]sqtypes.Value{rw1, rw1, rw2, rw3, rw4, rw5, rw6, rw7},
			SortOrderErr: "Error: Column colX not found in dataset",
		},
		{
			TestName: "Sort Dataset",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: vals,
			Order:    []sqtables.OrderItem{{ColName: "col2", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			ExpVals:  [][]sqtypes.Value{rw1, rw1, rw2, rw3, rw4, rw5, rw6, rw7},
		},
		{
			TestName: "Sort Empty Dataset",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: [][]sqtypes.Value{},
			Order:    []sqtables.OrderItem{{ColName: "col2", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			ExpVals:  [][]sqtypes.Value{},
		},
		{
			TestName: "Sort Dataset desc",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: vals,
			Order:    []sqtables.OrderItem{{ColName: "col2", SortType: tokens.Desc}, {ColName: "col1", SortType: tokens.Desc}},
			ExpVals:  [][]sqtypes.Value{rw7, rw6, rw5, rw4, rw3, rw2, rw1, rw1},
		},
		{
			TestName: "Sort Dataset desc/asc",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: vals,
			Order:    []sqtables.OrderItem{{ColName: "col2", SortType: tokens.Desc}, {ColName: "col1", SortType: tokens.Asc}},
			ExpVals:  [][]sqtypes.Value{rw6, rw7, rw5, rw4, rw3, rw2, rw1, rw1},
		},
		{
			TestName: "Sort Dataset no order",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: vals,
			Order:    nil,
			ExpVals:  [][]sqtypes.Value{rw6, rw7, rw5, rw4, rw3, rw2, rw1, rw1},
			SortErr:  "Error: Sort Order has not been set for DataSet",
		},
		{
			TestName: "Sort Dataset with Distinct",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: vals,
			Order:    nil,
			ExpVals:  [][]sqtypes.Value{rw1, rw2, rw3, rw4, rw5, rw6, rw7},
			Distinct: true,
		},
		{
			TestName: "Sort Dataset with nulls",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: valsWithNull,
			Order:    []sqtables.OrderItem{{ColName: "col2", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Asc}},
			ExpVals:  [][]sqtypes.Value{rw1, rw1, rw2, rw3, rw4, rw5, rw6, rw7, rwNil10, rwNil11, rwNil12},
		},
		{
			TestName: "Sort Dataset with nulls DESC",
			Tables:   tables,
			DataCols: exprCols,
			InitVals: valsWithNull,
			Order:    []sqtables.OrderItem{{ColName: "col2", SortType: tokens.Asc}, {ColName: "col1", SortType: tokens.Desc}},
			ExpVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"aaa", 6}, {"aaa", 6}, {"nnn", 1}, {"qab", 2}, {"qqq", 4}, {"qxc", 8}, {"ttt", 9}, {"ttt", 1}, {nil, 12}, {nil, 11}, {nil, 10},
			}),
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testSortFunc(row))

	}

}

type SortData struct {
	TestName     string
	Tables       *sqtables.TableList
	DataCols     *sqtables.ExprList
	InitVals     [][]sqtypes.Value
	Order        []sqtables.OrderItem
	ExpVals      [][]sqtypes.Value
	SortOrderErr string
	SortErr      string
	Distinct     bool
}

func testSortFunc(d SortData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()
		data, err := sqtables.NewDataSet(profile, d.Tables, d.DataCols, nil)
		if err != nil {
			t.Errorf("Unexpected Error in test: %s", err.Error())
			return
		}

		data.Vals = d.InitVals

		if d.Distinct {
			data.Distinct()
		}

		if !(d.Distinct && d.Order == nil) {
			err := data.SetOrder(d.Order)
			if sqtest.CheckErr(t, err, d.SortOrderErr) {
				return
			}

			err = data.Sort()
			if sqtest.CheckErr(t, err, d.SortErr) {
				return
			}

		}
		if d.ExpVals != nil {
			msg := sqtypes.Compare2DValue(data.Vals, d.ExpVals, "Actual", "Expect", false)
			if msg != "" {
				t.Error(msg)
				//fmt.Println(data.Vals)
				//fmt.Println(d.ExpVals)
				return
			}
		}

	}
}

type GroupByData struct {
	TestName      string
	Tables        *sqtables.TableList
	DataCols      *sqtables.ExprList
	InitVals      [][]sqtypes.Value
	Order         []sqtables.OrderItem
	ExpVals       [][]sqtypes.Value
	GroupBy       *sqtables.ExprList
	NewDataSetErr string
	ExpErr        string
	SortOrderErr  string
	SortErr       string
}

func testGroupByFunc(d GroupByData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()
		data, err := sqtables.NewDataSet(profile, d.Tables, d.DataCols, d.GroupBy)
		if sqtest.CheckErr(t, err, d.NewDataSetErr) {
			return
		}

		data.Vals = d.InitVals

		if d.GroupBy != nil || d.DataCols.HasAggregateFunc() {
			err = data.GroupBy()
		}

		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		//fmt.Println(data.Vals)
		if d.Order != nil {
			err := data.SetOrder(d.Order)
			if sqtest.CheckErr(t, err, d.SortOrderErr) {
				return
			}

			err = data.Sort()
			if sqtest.CheckErr(t, err, d.SortErr) {
				return
			}

		}
		if !reflect.DeepEqual(data.Vals, d.ExpVals) {
			fmt.Println("  Actual Values:", data.Vals)
			fmt.Println("Expected Values:", d.ExpVals)
			t.Error("The actual values after the Group By did not match expected values")
			return
		}

	}
}

func CreateTable(profile *sqprofile.SQProfile, str string) (*sqtables.TableDef, error) {
	tkns := tokens.Tokenize(str)
	tableName, err := cmd.CreateTableFromTokens(profile, tkns)
	if err != nil {
		return nil, err
	}

	return sqtables.GetTable(profile, tableName)
}

func TestGroupBy(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	tab1, err := CreateTable(profile, "CREATE TABLE testgroupby (firstname string, lastname string, age int, salary float, cityid int)")
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}
	tab2, err := CreateTable(profile, "CREATE TABLE testgroupbycity (cityid int, name string, country string)")
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	tables := sqtables.NewTableListFromTableDef(profile, tab1)
	firstnameCD := sqtables.ColDef{ColName: "firstname", ColType: tokens.String}
	lastnameCD := sqtables.ColDef{ColName: "lastname", ColType: tokens.String}
	firstNameExp := sqtables.NewColExpr(firstnameCD)
	lastNameExp := sqtables.NewColExpr(lastnameCD)
	multitable := sqtables.NewTableListFromTableDef(profile, tab1, tab2)
	cityNameCD := sqtables.ColDef{ColName: "name", ColType: tokens.String, TableName: tab2.GetName(profile), DisplayTableName: true}
	cityNameExp := sqtables.NewColExpr(cityNameCD)
	ageExp := sqtables.NewColExpr(sqtables.ColDef{ColName: "age", ColType: tokens.Int})
	data := []GroupByData{
		{
			TestName: "GroupBy Dataset No Group Cols",
			Tables:   tables,
			DataCols: sqtables.NewExprList(sqtables.NewColExpr(firstnameCD), sqtables.NewFuncExpr(tokens.Count, nil)),
			InitVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"fred", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
				{"whilma", nil},
				{"barney", nil},
				{"barney", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
			}),
			ExpVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"barney", 2},
				{"betty", 2},
				{"fred", 3},
				{"whilma", 1},
				{nil, 2},
			}),
			NewDataSetErr: "Syntax Error: Select Statements with Aggregate functions (count, sum, min, max, avg) must not have other expressions",
		},
		{
			TestName: "Dataset GroupBy firstname",
			Tables:   tables,
			DataCols: sqtables.NewExprList(sqtables.NewColExpr(firstnameCD), sqtables.NewFuncExpr(tokens.Count, nil)),
			InitVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"fred", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
				{"whilma", nil},
				{"barney", nil},
				{"barney", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
			}),
			ExpVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"barney", 2},
				{"betty", 2},
				{"fred", 3},
				{"whilma", 1},
				{nil, 2},
			}),
			GroupBy: sqtables.ColsToExpr(sqtables.NewColListDefs([]sqtables.ColDef{firstnameCD})),
			ExpErr:  "",
		},
		{
			TestName: "Dataset GroupBy first, last names",
			Tables:   tables,
			DataCols: sqtables.NewExprList(sqtables.NewColExpr(firstnameCD), sqtables.NewColExpr(lastnameCD), sqtables.NewFuncExpr(tokens.Count, nil)),
			InitVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"fred", "flintstone", nil},
				{nil, nil, nil},
				{"betty", "rubble", nil},
				{"fred", "flintstone", nil},
				{"whilma", "flintstone", nil},
				{"barney", "rubble", nil},
				{"barney", "rubble", nil},
				{nil, nil, nil},
				{"betty", "rubble", nil},
				{"fred", "mercury", nil},
			}),
			ExpVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"barney", "rubble", 2},
				{"betty", "rubble", 2},
				{"fred", "flintstone", 2},
				{"fred", "mercury", 1},
				{"whilma", "flintstone", 1},
				{nil, nil, 2},
			}),
			GroupBy: sqtables.NewExprList(firstNameExp, lastNameExp),
			ExpErr:  "",
		},
		{
			TestName: "Dataset GroupBy firstname, extra col in elist",
			Tables:   tables,
			DataCols: sqtables.NewExprList(sqtables.NewColExpr(firstnameCD), sqtables.NewColExpr(lastnameCD), sqtables.NewFuncExpr(tokens.Count, nil)),
			InitVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"fred", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
				{"whilma", nil},
				{"barney", nil},
				{"barney", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
			}),
			ExpVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"barney", 2},
				{"betty", 2},
				{"fred", 3},
				{"whilma", 1},
				{nil, 2},
			}),
			GroupBy:       sqtables.NewExprList(firstNameExp),
			NewDataSetErr: "Syntax Error: lastname is not in the group by clause: firstname",
		},
		{
			TestName: "Dataset GroupBy firstname non aggregate function",
			Tables:   tables,
			DataCols: sqtables.NewExprList(sqtables.NewColExpr(firstnameCD), sqtables.NewColExpr(lastnameCD), sqtables.NewFuncExpr(tokens.Count, nil)),
			InitVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"fred", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
				{"whilma", nil},
				{"barney", nil},
				{"barney", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
			}),
			ExpVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"barney", 2},
				{"betty", 2},
				{"fred", 3},
				{"whilma", 1},
				{nil, 2},
			}),
			GroupBy:       sqtables.NewExprList(firstNameExp),
			NewDataSetErr: "Syntax Error: lastname is not in the group by clause: firstname",
		},
		{
			TestName: "Dataset implicit group by",
			Tables:   tables,
			DataCols: sqtables.NewExprList(sqtables.NewFuncExpr(tokens.Count, nil)),
			InitVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"fred"},
				{nil},
				{"betty"},
				{"fred"},
				{"whilma"},
				{"barney"},
				{"barney"},
				{nil},
				{"betty"},
				{"fred"},
			}),
			ExpVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{10},
			}),
			GroupBy:       nil,
			NewDataSetErr: "",
		},
		{
			TestName: "Dataset implicit group by with count, sum, min, max, avg",
			Tables:   tables,
			DataCols: sqtables.NewExprList(
				sqtables.NewFuncExpr(tokens.Count, nil),
				sqtables.NewFuncExpr(tokens.Sum,
					sqtables.NewColExpr(sqtables.ColDef{ColName: "age"}),
				),
				sqtables.NewFuncExpr(tokens.Min,
					sqtables.NewColExpr(sqtables.ColDef{ColName: "age"}),
				),
				sqtables.NewFuncExpr(tokens.Max,
					sqtables.NewColExpr(sqtables.ColDef{ColName: "age"}),
				),
				sqtables.NewFuncExpr(tokens.Avg,
					sqtables.NewColExpr(sqtables.ColDef{ColName: "age"}),
				),
			),
			InitVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"fred", 10, 10, 10, 10},
				{nil, nil, nil, nil, nil},
				{"betty", 20, 20, 20, 20},
				{"fred", 10, 10, 10, 10},
				{"whilma", 20, 20, 20, 20},
				{"barney", 11, 5, 5, 11},
				{"barney", 11, 11, 11, 11},
				{nil, nil, nil, nil, nil},
				{"betty", 21, 21, 21, 21},
				{"fred", 75, 75, 75, 75},
			}),
			ExpVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{10, 178, 5, 75, 22.25},
			}),
			GroupBy:       nil,
			NewDataSetErr: "",
		},
		{
			TestName: "Dataset multi table group by city.name with count, sum, min, max, avg",
			Tables:   multitable,
			DataCols: sqtables.NewExprList(
				cityNameExp,
				sqtables.NewFuncExpr(tokens.Count, nil),
				sqtables.NewFuncExpr(tokens.Sum,
					sqtables.NewColExpr(sqtables.ColDef{ColName: "age"}),
				),
				sqtables.NewFuncExpr(tokens.Min,
					sqtables.NewColExpr(sqtables.ColDef{ColName: "age"}),
				),
				sqtables.NewFuncExpr(tokens.Max,
					sqtables.NewColExpr(sqtables.ColDef{ColName: "age"}),
				),
				sqtables.NewFuncExpr(tokens.Avg,
					sqtables.NewColExpr(sqtables.ColDef{ColName: "age"}),
				),
			),
			InitVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"Toronto", nil, 25, 25, 25, 25},
				{"Ottawa", nil, 75, 75, 75, 75},
				{"Barrie", nil, 16, 16, 16, 16},
				{"Toronto", nil, 3, 3, 3, 3},
				{"Ottawa", nil, 28, 28, 28, 28},
				{"Toronto", nil, 31, 31, 31, 31},
			}),
			ExpVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"Barrie", 1, 16, 16, 16, 16.0},
				{"Ottawa", 2, 103, 28, 75, 51.5},
				{"Toronto", 3, 59, 3, 31, 19.666666666666668},
			}),
			GroupBy:       sqtables.NewExprList(cityNameExp),
			NewDataSetErr: "",
		},
		{
			TestName: "Dataset GroupBy invalid",
			Tables:   tables,
			DataCols: sqtables.NewExprList(sqtables.NewColExpr(firstnameCD), sqtables.NewFuncExpr(tokens.Count, nil)),
			InitVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"fred", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
				{"whilma", nil},
				{"barney", nil},
				{"barney", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
			}),
			ExpVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"barney", 2},
				{"betty", 2},
				{"fred", 3},
				{"whilma", 1},
				{nil, 2},
			}),
			GroupBy:       sqtables.ColsToExpr(sqtables.NewColListDefs([]sqtables.ColDef{cityNameCD})),
			NewDataSetErr: "Error: Table testgroupbycity not found in table list",
		},
		{
			TestName: "Dataset GroupBy invalid not in elist",
			Tables:   tables,
			DataCols: sqtables.NewExprList(sqtables.NewColExpr(firstnameCD), sqtables.NewFuncExpr(tokens.Count, nil)),
			InitVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"fred", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
				{"whilma", nil},
				{"barney", nil},
				{"barney", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
			}),
			ExpVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"barney", 2},
				{"betty", 2},
				{"fred", 3},
				{"whilma", 1},
				{nil, 2},
			}),
			GroupBy:       sqtables.ColsToExpr(sqtables.NewColListDefs([]sqtables.ColDef{firstnameCD, lastnameCD})),
			NewDataSetErr: "Syntax Error: lastname is not in the expression list: firstname,COUNT()",
		},
		{
			TestName: "Dataset GroupBy non aggregate function",
			Tables:   tables,
			DataCols: sqtables.NewExprList(sqtables.NewColExpr(firstnameCD), sqtables.NewFuncExpr(tokens.String, ageExp)),
			InitVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"fred", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
				{"whilma", nil},
				{"barney", nil},
				{"barney", nil},
				{nil, nil},
				{"betty", nil},
				{"fred", nil},
			}),
			ExpVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"barney", 2},
				{"betty", 2},
				{"fred", 3},
				{"whilma", 1},
				{nil, 2},
			}),
			GroupBy:       sqtables.ColsToExpr(sqtables.NewColListDefs([]sqtables.ColDef{firstnameCD})),
			NewDataSetErr: "Syntax Error: STRING(age) is not an aggregate function",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGroupByFunc(row))

	}
}
