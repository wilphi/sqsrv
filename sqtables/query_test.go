package sqtables_test

import (
	"fmt"
	"reflect"
	"testing"

	log "github.com/sirupsen/logrus"
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

////////////////////////////////////////////////////////////////
func TestQueryGetRowData(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	//var err error
	profile := sqprofile.CreateSQProfile()

	tCountry, err := sqtables.CreateTableFromRawFile(profile, "./testdata/query/country.txt", "country")
	if err != nil {
		panic(err)
	}

	tCity, err := sqtables.CreateTableFromRawFile(profile, "./testdata/query/city.txt", "city")
	if err != nil {
		panic(err)
	}

	tPerson, err := sqtables.CreateTableFromRawFile(profile, "./testdata/query/person.txt", "person")
	if err != nil {
		panic(err)
	}

	tList := sqtables.NewTableList(profile,
		[]sqtables.TableRef{*tPerson, *tCity, *tCountry})
	eList := sqtables.ColsToExpr(
		column.NewListNames(
			[]string{
				"firstname",
				"lastname",
				"city.name",
				"city.prov",
				"country.short",
			},
		),
	)
	whereExpr :=
		sqtables.NewOpExpr(
			sqtables.NewColExpr(column.Ref{ColName: "short", ColType: tokens.String, TableName: tCountry.Name.Clone()}),
			tokens.NotEqual,
			sqtables.NewValueExpr(sqtypes.NewSQString("USA")),
		)

	whereExpr2 :=
		sqtables.NewOpExpr(
			sqtables.NewColExpr(column.Ref{ColName: "country", ColType: tokens.String, TableName: tCity.Name.Clone()}),
			tokens.NotEqual,
			sqtables.NewValueExpr(sqtypes.NewSQString("United States")),
		)

	joins := []sqtables.JoinInfo{
		{
			TableA:   *tCity,
			TableB:   *tCountry,
			JoinType: tokens.Inner,
			ONClause: sqtables.NewOpExpr(
				sqtables.NewColExpr(column.Ref{ColName: "country", ColType: tokens.String, TableName: tCity.Name.Clone()}),
				tokens.Equal,
				sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: moniker.New("country", "")}),
			),
		},
		{
			TableA:   *tCity,
			TableB:   *tPerson,
			JoinType: tokens.Inner,
			ONClause: sqtables.NewOpExpr(
				sqtables.NewColExpr(column.Ref{ColName: "cityid", ColType: tokens.Int, TableName: moniker.New("city", "")}),
				tokens.Equal,
				sqtables.NewColExpr(column.Ref{ColName: "cityid", ColType: tokens.Int, TableName: moniker.New("Person", "")}),
			),
		},
	}

	data := []QueryGetRowData{

		{
			TestName: "City/Country Query",
			Query: sqtables.Query{
				Tables:    sqtables.NewTableList(profile, []sqtables.TableRef{*tCity, *tCountry}),
				EList:     sqtables.ColsToExpr(column.NewListNames([]string{"city.name", "city.prov", "country.short"})),
				WhereExpr: whereExpr,
				Joins:     joins[:1],
			},
			ExpErr:      "",
			ExpValsPath: "./testdata/query/results/citycountryquery.txt",
		},
		{
			TestName: "City/Person Query",
			Query: sqtables.Query{
				Tables:    sqtables.NewTableList(profile, []sqtables.TableRef{*tPerson, *tCity}),
				EList:     sqtables.ColsToExpr(column.NewListNames([]string{"firstname", "lastname", "city.name", "city.prov"})),
				WhereExpr: whereExpr2,
				Joins:     joins[1:],
			},
			ExpErr:      "",
			ExpValsPath: "./testdata/query/results/citypersonquery.txt",
		},

		{
			TestName: "Multitable Query",
			Query: sqtables.Query{
				Tables:    tList,
				EList:     eList,
				WhereExpr: whereExpr,
				Joins:     joins,
			},
			ExpErr:      "",
			ExpValsPath: "./testdata/query/results/multitablequery.txt",
		},
		{
			TestName: "Nil Expression List",
			Query: sqtables.Query{
				Tables:    tList,
				EList:     nil,
				WhereExpr: whereExpr,
				Joins:     joins,
			},
			ExpErr: "Internal Error: Expression List must have at least one item",
		},
		{
			TestName: "Empty Expression List",
			Query: sqtables.Query{
				Tables:    tList,
				EList:     sqtables.NewExprList(),
				WhereExpr: whereExpr,
				Joins:     joins,
			},
			ExpErr: "Internal Error: Expression List must have at least one item",
		},
		{
			TestName: "Invalid colName in Expression List",
			Query: sqtables.Query{
				Tables:    tList,
				EList:     sqtables.NewExprList(sqtables.NewColExpr(column.Ref{ColName: "colX"})),
				WhereExpr: whereExpr,
				Joins:     joins,
			},
			ExpErr: "Error: Column \"colX\" not found in Table(s): city, country, person",
		},
		{
			TestName: "Invalid tablename in Expression List",
			Query: sqtables.Query{
				Tables:    tList,
				EList:     sqtables.NewExprList(sqtables.NewColExpr(column.Ref{ColName: "name", TableName: moniker.New("NotATable", "")})),
				WhereExpr: whereExpr,
				Joins:     joins,
			},
			ExpErr: "Error: Table notatable not found in table list",
		},
		{
			TestName: "Empty Table List",
			Query: sqtables.Query{
				Tables:    sqtables.NewTableList(profile, nil),
				EList:     eList,
				WhereExpr: whereExpr,
				Joins:     joins,
			},
			ExpErr: "Internal Error: TableList must not be empty for query",
		},
		{
			TestName: "Multitable Query No Where clause",
			Query: sqtables.Query{
				Tables:    tList,
				EList:     eList,
				WhereExpr: nil,
				Joins: []sqtables.JoinInfo{
					{
						TableA:   *tCity,
						TableB:   *tCountry,
						JoinType: tokens.Inner,
						ONClause: sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "country", ColType: tokens.String, TableName: moniker.New("city", "")}),
							tokens.Equal,
							sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: moniker.New("country", "")}),
						),
					},
					{
						TableA:   *tCity,
						TableB:   *tPerson,
						JoinType: tokens.Inner,
						ONClause: sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "cityid", ColType: tokens.Int, TableName: moniker.New("city", "")}),
							tokens.Equal,
							sqtables.NewColExpr(column.Ref{ColName: "cityid", ColType: tokens.Int, TableName: moniker.New("Person", "")}),
						),
					},
				},
			},
			ExpErr:      "",
			ExpValsPath: "./testdata/query/results/multitablequerynowhereclause.txt",
		},
		{
			TestName: "Multitable Query err in Where clause",
			Query: sqtables.Query{
				Tables:    tList,
				EList:     eList,
				WhereExpr: sqtables.NewColExpr(column.Ref{ColName: "colX"}),
				Joins:     joins,
			},
			ExpErr: "Error: Column \"colX\" not found in Table(s): city, country, person",
		},
		{
			TestName: "Multitable Query Count()",
			Query: sqtables.Query{
				Tables:    tList,
				EList:     sqtables.NewExprList(sqtables.NewFuncExpr(tokens.Count, nil)),
				WhereExpr: whereExpr,
				Joins: []sqtables.JoinInfo{
					{
						TableA:   *tCity,
						TableB:   *tCountry,
						JoinType: tokens.Inner,
						ONClause: sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "country", ColType: tokens.String, TableName: moniker.New("city", "")}),
							tokens.Equal,
							sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: moniker.New("country", "")}),
						),
					},
					{
						TableA:   *tCity,
						TableB:   *tPerson,
						JoinType: tokens.Inner,
						ONClause: sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "cityid", ColType: tokens.Int, TableName: moniker.New("city", "")}),
							tokens.Equal,
							sqtables.NewColExpr(column.Ref{ColName: "cityid", ColType: tokens.Int, TableName: moniker.New("Person", "")}),
						),
					},
				},
			},
			ExpErr: "",
			ExpVals: sqtypes.RawVals{
				{16},
			},
		},

		{
			TestName: "Multitable Query Cross Join Count()",
			Query: sqtables.Query{
				Tables: tList,
				EList:  sqtables.NewExprList(sqtables.NewFuncExpr(tokens.Count, nil)),
				WhereExpr: sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: moniker.New("Person", "")}),
							tokens.Equal,
							sqtables.NewValueExpr(sqtypes.NewSQString("Ava")),
						),
						tokens.Or,
						sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: moniker.New("Person", "")}),
							tokens.Equal,
							sqtables.NewValueExpr(sqtypes.NewSQString("Luna")),
						),
					),
					tokens.And,
					sqtables.NewOpExpr(
						sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: moniker.New("city", "")}),
						tokens.Equal,
						sqtables.NewValueExpr(sqtypes.NewSQString("Springfield")),
					),
				),

				Joins: []sqtables.JoinInfo{
					{
						TableA:   *tCity,
						TableB:   *tCountry,
						JoinType: tokens.Cross,
						ONClause: nil,
					},
					{
						TableA:   *tCity,
						TableB:   *tPerson,
						JoinType: tokens.Cross,
						ONClause: nil,
					},
				},
			},
			ExpErr: "",
			ExpVals: sqtypes.RawVals{
				{16},
			},
		},

		{
			TestName: "Multitable Query Cross Join with Cols",
			Query: sqtables.Query{
				Tables: tList,
				EList: sqtables.NewExprList(
					sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: moniker.New("Person", "")}),
					sqtables.NewColExpr(column.Ref{ColName: "lastname", ColType: tokens.String, TableName: moniker.New("Person", "")}),
					sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: moniker.New("city", "")}),
					sqtables.NewColExpr(column.Ref{ColName: "country", ColType: tokens.String, TableName: moniker.New("city", "")}),
					sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: moniker.New("country", "")}),
				),
				WhereExpr: sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: moniker.New("Person", "")}),
							tokens.Equal,
							sqtables.NewValueExpr(sqtypes.NewSQString("Ava")),
						),
						tokens.Or,
						sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: moniker.New("Person", "")}),
							tokens.Equal,
							sqtables.NewValueExpr(sqtypes.NewSQString("Luna")),
						),
					),
					tokens.And,
					sqtables.NewOpExpr(
						sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: moniker.New("city", "")}),
						tokens.Equal,
						sqtables.NewValueExpr(sqtypes.NewSQString("Springfield")),
					),
				),
				Joins: []sqtables.JoinInfo{
					{
						TableA:   *tCity,
						TableB:   *tCountry,
						JoinType: tokens.Cross,
						ONClause: nil,
					},
					{
						TableA:   *tCity,
						TableB:   *tPerson,
						JoinType: tokens.Cross,
						ONClause: nil,
					},
				},
			},
			ExpErr:      "",
			ExpValsPath: "./testdata/query/results/multitablequerycrossjoinwithcols.txt",
		},

		{
			TestName: "Single table Query",
			Query: sqtables.Query{
				Tables:    sqtables.NewTableList(profile, []sqtables.TableRef{{Name: moniker.New("country", "")}}),
				EList:     sqtables.NewExprList(sqtables.NewColExpr(column.NewRef("short", tokens.String, false))),
				WhereExpr: nil,
				Joins:     nil,
			},
			ExpErr: "",
			ExpVals: sqtypes.RawVals{
				{"GBR"},
				{"USA"},
				{"CAN"},
				{"FRA"},
			},
		},
		{
			TestName: "Single table Count() Query",
			Query: sqtables.Query{
				Tables:    sqtables.NewTableList(profile, []sqtables.TableRef{{Name: moniker.New("country", "")}}),
				EList:     sqtables.NewExprList(sqtables.NewFuncExpr(tokens.Count, nil)),
				WhereExpr: nil,
				Joins:     nil,
			},
			ExpErr: "",
			ExpVals: sqtypes.RawVals{
				{4},
			},
		},
		{
			TestName: "Single table City Count() Query",
			Query: sqtables.Query{
				Tables:    sqtables.NewTableList(profile, []sqtables.TableRef{{Name: moniker.New("city", "")}}),
				EList:     sqtables.NewExprList(sqtables.NewFuncExpr(tokens.Count, nil)),
				WhereExpr: nil,
				Joins:     nil,
			},
			ExpErr: "",
			ExpVals: sqtypes.RawVals{
				{55},
			},
		},
		{
			TestName: "Single table Count() Query group by country",
			Query: sqtables.Query{
				Tables:    sqtables.NewTableList(profile, []sqtables.TableRef{{Name: moniker.New("city", "")}}),
				EList:     sqtables.NewExprList(sqtables.NewColExpr(column.NewRef("country", tokens.String, false)), sqtables.NewFuncExpr(tokens.Count, nil)),
				WhereExpr: nil,
				Joins:     nil,
				GroupBy:   sqtables.NewExprList(sqtables.NewColExpr(column.NewRef("country", tokens.NilToken, false))),
			},
			ExpErr: "",
			ExpVals: sqtypes.RawVals{
				{"Canada", 2},
				{"Switzerland", 1},
				{"United Kingdom", 4},
				{"United States", 48},
			},
		},
		{
			TestName: "Multitable Query Cross Join with Cols Group By firstname",
			Query: sqtables.Query{
				Tables: tList,
				EList: sqtables.NewExprList(
					sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: moniker.New("Person", "")}),
					sqtables.NewFuncExpr(tokens.Count, nil),
				),
				WhereExpr: sqtables.NewOpExpr(
					sqtables.NewOpExpr(
						sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: moniker.New("Person", "")}),
							tokens.Equal,
							sqtables.NewValueExpr(sqtypes.NewSQString("Ava")),
						),
						tokens.Or,
						sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "firstname", ColType: tokens.String, TableName: moniker.New("Person", "")}),
							tokens.Equal,
							sqtables.NewValueExpr(sqtypes.NewSQString("Luna")),
						),
					),
					tokens.And,
					sqtables.NewOpExpr(
						sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.String, TableName: moniker.New("city", "")}),
						tokens.Equal,
						sqtables.NewValueExpr(sqtypes.NewSQString("Springfield")),
					),
				),
				Joins: []sqtables.JoinInfo{
					{
						TableA:   *tCity,
						TableB:   *tCountry,
						JoinType: tokens.Cross,
						ONClause: nil,
					},
					{
						TableA:   *tCity,
						TableB:   *tPerson,
						JoinType: tokens.Cross,
						ONClause: nil,
					},
				},
				GroupBy: sqtables.NewExprList(sqtables.NewColExpr(column.NewRef("firstname", tokens.NilToken, false))),
			},
			ExpErr: "",
			ExpVals: sqtypes.RawVals{
				{"Ava", 8},
				{"Luna", 8},
			},
		},

		{
			TestName: "Left Outer Join",
			Query: sqtables.Query{
				Tables: sqtables.NewTableList(profile, []sqtables.TableRef{*tCountry, *tCity}),
				EList: sqtables.NewExprList(
					sqtables.NewColExpr(column.Ref{ColName: "short", ColType: tokens.Int, TableName: moniker.New("country", "")}),
					sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.Int, TableName: moniker.New("city", "")}),
				),
				WhereExpr: nil,
				Joins: []sqtables.JoinInfo{
					{
						TableA:   *tCountry,
						TableB:   *tCity,
						JoinType: tokens.Left,
						ONClause: sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.Int, TableName: moniker.New("country", "")}),
							tokens.Equal,
							sqtables.NewColExpr(column.Ref{ColName: "country", ColType: tokens.Int, TableName: moniker.New("city", "")}),
						),
					},
				},
			},
			ExpErr:      "",
			ExpValsPath: "./testdata/query/results/leftouterjoin.txt",
		},
		{
			TestName: "Right Outer Join coverted to Left",
			Query: sqtables.Query{
				Tables: sqtables.NewTableList(profile, []sqtables.TableRef{*tCity, *tCountry}),
				EList: sqtables.NewExprList(
					sqtables.NewColExpr(column.Ref{ColName: "short", ColType: tokens.Int, TableName: moniker.New("country", "")}),
					sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.Int, TableName: moniker.New("city", "")}),
				),
				WhereExpr: nil,
				Joins: []sqtables.JoinInfo{
					{
						TableA:   *tCity,
						TableB:   *tCountry,
						JoinType: tokens.Right,
						ONClause: sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.Int, TableName: moniker.New("country", "")}),
							tokens.Equal,
							sqtables.NewColExpr(column.Ref{ColName: "country", ColType: tokens.Int, TableName: moniker.New("city", "")}),
						),
					},
				},
			},
			ExpErr:      "",
			ExpValsPath: "./testdata/query/results/leftouterjoin.txt",
		},
		{
			TestName: "Right Outer Join",
			Query: sqtables.Query{
				Tables: sqtables.NewTableList(profile, []sqtables.TableRef{*tCountry, *tCity}),
				EList: sqtables.NewExprList(
					sqtables.NewColExpr(column.Ref{ColName: "short", ColType: tokens.Int, TableName: moniker.New("country", "")}),
					sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.Int, TableName: moniker.New("city", "")}),
				),
				WhereExpr: nil,
				Joins: []sqtables.JoinInfo{
					{
						TableA:   *tCountry,
						TableB:   *tCity,
						JoinType: tokens.Right,
						ONClause: sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.Int, TableName: moniker.New("country", "")}),
							tokens.Equal,
							sqtables.NewColExpr(column.Ref{ColName: "country", ColType: tokens.Int, TableName: moniker.New("city", "")}),
						),
					},
				},
			},
			ExpErr:      "",
			ExpValsPath: "./testdata/query/results/rightouterjoin.txt",
		},
		{
			TestName: "Right Outer Join no country",
			Query: sqtables.Query{
				Tables: sqtables.NewTableList(profile, []sqtables.TableRef{*tCountry, *tCity}),
				EList: sqtables.NewExprList(
					sqtables.NewColExpr(column.Ref{ColName: "short", ColType: tokens.Int, TableName: moniker.New("country", "")}),
					sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.Int, TableName: moniker.New("city", "")}),
				),
				WhereExpr: sqtables.NewOpExpr(
					sqtables.NewColExpr(column.Ref{ColName: "short", ColType: tokens.Int, TableName: moniker.New("country", "")}),
					tokens.Equal,
					sqtables.NewValueExpr(sqtypes.NewSQString("BAH")),
				),
				Joins: []sqtables.JoinInfo{
					{
						TableA:   *tCountry,
						TableB:   *tCity,
						JoinType: tokens.Right,
						ONClause: sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.Int, TableName: moniker.New("country", "")}),
							tokens.Equal,
							sqtables.NewColExpr(column.Ref{ColName: "country", ColType: tokens.Int, TableName: moniker.New("city", "")}),
						),
					},
				},
			},
			ExpErr:      "",
			ExpValsPath: "./testdata/query/results/rightouterjoinnocountry.txt",
		},
		{
			TestName: "Full Outer Join",
			Query: sqtables.Query{
				Tables: sqtables.NewTableList(profile, []sqtables.TableRef{*tCountry, *tCity}),
				EList: sqtables.NewExprList(
					sqtables.NewColExpr(column.Ref{ColName: "short", ColType: tokens.Int, TableName: moniker.New("country", "")}),
					sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.Int, TableName: moniker.New("city", "")}),
				),
				WhereExpr: nil,
				Joins: []sqtables.JoinInfo{
					{
						TableA:   *tCountry,
						TableB:   *tCity,
						JoinType: tokens.Full,
						ONClause: sqtables.NewOpExpr(
							sqtables.NewColExpr(column.Ref{ColName: "name", ColType: tokens.Int, TableName: moniker.New("country", "")}),
							tokens.Equal,
							sqtables.NewColExpr(column.Ref{ColName: "country", ColType: tokens.Int, TableName: moniker.New("city", "")}),
						),
					},
				},
			},
			ExpErr:      "",
			ExpValsPath: "./testdata/query/results/fullouterjoin.txt",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testQueryGetRowDataFunc(row))

	}

}

type QueryGetRowData struct {
	TestName    string
	Query       sqtables.Query
	ExpErr      string
	ExpVals     sqtypes.RawVals
	ExpValsPath string
}

func testQueryGetRowDataFunc(d QueryGetRowData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		var expVals [][]sqtypes.Value

		profile := sqprofile.CreateSQProfile()
		data, err := d.Query.GetRowData(profile)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}
		if d.ExpValsPath != "" {
			expRaw, err := sqtables.ReadRawFromFile(d.ExpValsPath)
			if err != nil {
				log.Panic(err)
			}
			expVals = sqtypes.CreateValuesFromRaw(expRaw)
		} else {
			expVals = sqtypes.CreateValuesFromRaw(d.ExpVals)
		}

		msg := sqtypes.Compare2DValue(data.Vals, expVals, "Actual", "Expect", true)
		if msg != "" {
			t.Error(msg)
			return
		}
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//

type GroupByData struct {
	TestName      string
	Query         *sqtables.Query
	InitVals      [][]sqtypes.Value
	Order         []sqtables.OrderItem
	ExpVals       [][]sqtypes.Value
	NewDataSetErr string
	ExpErr        string
	SortOrderErr  string
	SortErr       string
}

func testGroupByFunc(d GroupByData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()
		// Make sure groupby, having clause and eList follow rules for group by (if there is one)
		err := d.Query.ValidateGroupBySemantics(profile)
		if err != nil { // dont check for no error
			if sqtest.CheckErr(t, err, d.NewDataSetErr) {
				return
			}
		}

		data, err := sqtables.NewDataSet(profile, d.Query.Tables, d.Query.EList)
		if sqtest.CheckErr(t, err, d.NewDataSetErr) {
			return
		}

		data.Vals = d.InitVals

		if d.Query.GroupBy != nil || d.Query.EList.HasAggregateFunc() {
			err = d.Query.ProcessGroupBy(profile, data)
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
			//fmt.Println("  Actual Values:", data.Vals)
			//fmt.Println("Expected Values:", d.ExpVals)
			t.Error("The actual values after the Group By did not match expected values")
			return
		}

	}
}

func TestGroupBy(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	firstnameCD := column.NewDef("firstname", tokens.String, false)
	lastnameCD := column.NewDef("lastname", tokens.String, false)
	ageCD := column.NewDef("age", tokens.Int, false)

	tab1 := sqtables.CreateTableDef("testgroupby",
		[]column.Def{
			firstnameCD,
			lastnameCD,
			ageCD,
			column.NewDef("salary", tokens.Float, false),
			column.NewDef("cityid", tokens.Int, false),
		},
	)
	err := sqtables.CreateTable(profile, tab1)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}
	tab2 := sqtables.CreateTableDef("testgroupbycity",
		[]column.Def{
			column.NewDef("cityid", tokens.Int, false),
			column.NewDef("name", tokens.String, false),
			column.NewDef("country", tokens.String, false),
		},
	)
	err = sqtables.CreateTable(profile, tab2)
	if err != nil {
		t.Error("Error creating table: ", err)
		return
	}

	tables := sqtables.NewTableListFromTableDef(profile, tab1)
	firstNameExp := sqtables.NewColExpr(firstnameCD.Ref())
	lastNameExp := sqtables.NewColExpr(lastnameCD.Ref())
	multitable := sqtables.NewTableListFromTableDef(profile, tab1, tab2)
	cityNameCD := column.Ref{ColName: "name", ColType: tokens.String, TableName: tab2.TableRef(profile).Name, DisplayTableName: true}
	cityNameExp := sqtables.NewColExpr(cityNameCD)
	ageExp := sqtables.NewColExpr(column.Ref{ColName: "age", ColType: tokens.Int})

	having1 := sqtables.NewOpExpr(
		sqtables.NewFuncExpr(tokens.Count, nil),
		tokens.GreaterThan,
		sqtables.NewValueExpr(sqtypes.NewSQInt(1)),
	)
	data := []GroupByData{
		{
			TestName: "GroupBy Dataset No Group Cols",
			Query: &sqtables.Query{
				Tables: tables,
				EList:  sqtables.NewExprList(firstNameExp, sqtables.NewFuncExpr(tokens.Count, nil)),
			},
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
			Query: &sqtables.Query{
				Tables:  tables,
				EList:   sqtables.NewExprList(firstNameExp, sqtables.NewFuncExpr(tokens.Count, nil)),
				GroupBy: sqtables.ColsToExpr(column.NewListRefs([]column.Ref{firstnameCD.Ref()})),
			},
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
			ExpErr: "",
		},
		{
			TestName: "Dataset GroupBy first, last names",
			Query: &sqtables.Query{
				Tables:  tables,
				EList:   sqtables.NewExprList(firstNameExp, lastNameExp, sqtables.NewFuncExpr(tokens.Count, nil)),
				GroupBy: sqtables.NewExprList(firstNameExp, lastNameExp),
			},
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
			ExpErr: "",
		},
		{
			TestName: "Dataset GroupBy firstname, extra col in elist",
			Query: &sqtables.Query{
				Tables:  tables,
				EList:   sqtables.NewExprList(firstNameExp, lastNameExp, sqtables.NewFuncExpr(tokens.Count, nil)),
				GroupBy: sqtables.NewExprList(firstNameExp),
			},
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
			NewDataSetErr: "Syntax Error: lastname is not in the group by clause: firstname",
		},
		{
			TestName: "Dataset GroupBy firstname non aggregate function",
			Query: &sqtables.Query{
				Tables:  tables,
				EList:   sqtables.NewExprList(firstNameExp, lastNameExp, sqtables.NewFuncExpr(tokens.Count, nil)),
				GroupBy: sqtables.NewExprList(firstNameExp),
			},
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
			NewDataSetErr: "Syntax Error: lastname is not in the group by clause: firstname",
		},
		{
			TestName: "Dataset implicity GroupBy int(firstname) non aggregate function",
			Query: &sqtables.Query{
				Tables:  tables,
				EList:   sqtables.NewExprList(sqtables.NewFuncExpr(tokens.Int, lastNameExp), sqtables.NewFuncExpr(tokens.Count, nil)),
				GroupBy: nil,
			},
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
			NewDataSetErr: "Syntax Error: INT(lastname) is not an aggregate function",
		},
		{
			TestName: "Dataset implicit group by",
			Query: &sqtables.Query{
				Tables:  tables,
				EList:   sqtables.NewExprList(sqtables.NewFuncExpr(tokens.Count, nil)),
				GroupBy: nil,
			},
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
			NewDataSetErr: "",
		},
		{
			TestName: "Dataset implicit group by with count, sum, min, max, avg",
			Query: &sqtables.Query{
				Tables: tables,
				EList: sqtables.NewExprList(
					sqtables.NewFuncExpr(tokens.Count, nil),
					sqtables.NewFuncExpr(tokens.Sum,
						sqtables.NewColExpr(column.Ref{ColName: "age"}),
					),
					sqtables.NewFuncExpr(tokens.Min,
						sqtables.NewColExpr(column.Ref{ColName: "age"}),
					),
					sqtables.NewFuncExpr(tokens.Max,
						sqtables.NewColExpr(column.Ref{ColName: "age"}),
					),
					sqtables.NewFuncExpr(tokens.Avg,
						sqtables.NewColExpr(column.Ref{ColName: "age"}),
					),
				),
				GroupBy: nil,
			},
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
			NewDataSetErr: "",
		},
		{
			TestName: "Dataset multi table group by city.name with count, sum, min, max, avg",
			Query: &sqtables.Query{
				Tables: multitable,
				EList: sqtables.NewExprList(
					cityNameExp,
					sqtables.NewFuncExpr(tokens.Count, nil),
					sqtables.NewFuncExpr(tokens.Sum,
						sqtables.NewColExpr(column.Ref{ColName: "age"}),
					),
					sqtables.NewFuncExpr(tokens.Min,
						sqtables.NewColExpr(column.Ref{ColName: "age"}),
					),
					sqtables.NewFuncExpr(tokens.Max,
						sqtables.NewColExpr(column.Ref{ColName: "age"}),
					),
					sqtables.NewFuncExpr(tokens.Avg,
						sqtables.NewColExpr(column.Ref{ColName: "age"}),
					),
				),
				GroupBy: sqtables.NewExprList(cityNameExp),
			},
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
			NewDataSetErr: "",
		},
		{
			TestName: "Dataset GroupBy invalid",
			Query: &sqtables.Query{
				Tables:  tables,
				EList:   sqtables.NewExprList(firstNameExp, sqtables.NewFuncExpr(tokens.Count, nil)),
				GroupBy: sqtables.ColsToExpr(column.NewListRefs([]column.Ref{cityNameCD})),
			},
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
			NewDataSetErr: "Error: Table testgroupbycity not found in table list",
		},
		{
			TestName: "Dataset GroupBy invalid not in elist",
			Query: &sqtables.Query{
				Tables:  tables,
				EList:   sqtables.NewExprList(firstNameExp, sqtables.NewFuncExpr(tokens.Count, nil)),
				GroupBy: sqtables.ColsToExpr(column.NewListDefs([]column.Def{firstnameCD, lastnameCD})),
			},
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
			NewDataSetErr: "Syntax Error: lastname is not in the expression list: firstname,COUNT()",
		},
		{
			TestName: "Dataset GroupBy non aggregate function",
			Query: &sqtables.Query{
				Tables:  tables,
				EList:   sqtables.NewExprList(firstNameExp, sqtables.NewFuncExpr(tokens.String, ageExp)),
				GroupBy: sqtables.ColsToExpr(column.NewListDefs([]column.Def{firstnameCD})),
			},
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
			NewDataSetErr: "Syntax Error: STRING(age) is not an aggregate function",
		},
		{
			TestName: "Dataset GroupBy first, last names having count>1",
			Query: &sqtables.Query{
				Tables:     tables,
				EList:      sqtables.NewExprList(firstNameExp, lastNameExp, sqtables.NewFuncExpr(tokens.Count, nil)),
				GroupBy:    sqtables.NewExprList(firstNameExp, lastNameExp),
				HavingExpr: &having1,
			},
			InitVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"fred", "flintstone", nil, nil},
				{nil, nil, nil, nil},
				{"betty", "rubble", nil, nil},
				{"fred", "flintstone", nil, nil},
				{"whilma", "flintstone", nil, nil},
				{"barney", "rubble", nil, nil},
				{"barney", "rubble", nil, nil},
				{nil, nil, nil, nil},
				{"betty", "rubble", nil, nil},
				{"fred", "mercury", nil, nil},
			}),
			ExpVals: sqtypes.CreateValuesFromRaw(sqtypes.RawVals{
				{"barney", "rubble", 2},
				{"betty", "rubble", 2},
				{"fred", "flintstone", 2},
				{nil, nil, 2},
			}),
			ExpErr: "",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testGroupByFunc(row))

	}
}
