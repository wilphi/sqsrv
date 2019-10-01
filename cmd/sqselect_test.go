package cmd_test

import (
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

func init() {
	sqtest.TestInit("cmd_test.log")
}

type SelectData struct {
	TestName string
	Command  string
	ExpErr   string
	ExpRows  int
	ExpCols  []string
	ExpVals  sqtypes.RawVals
}

func testSelectFunc(profile *sqprofile.SQProfile, d SelectData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				_, fn, line, _ := runtime.Caller(4)
				t.Errorf("%s panicked unexpectedly: %s:%d %v", t.Name(), fn, line, r)
			}
		}()
		tkns := tokens.Tokenize(d.Command)
		_, data, err := cmd.Select(profile, tkns)
		if err != nil {
			log.Println(err.Error())
			if d.ExpErr == "" {
				t.Errorf("Unexpected Error in test: %s", err.Error())
				return
			}
			if d.ExpErr != err.Error() {
				t.Errorf("Expecting Error %s but got: %s", d.ExpErr, err.Error())
				return
			}
			return
		}
		if data.Len() != d.ExpRows {
			t.Errorf("The number of rows returned (%d) does not match expected rows (%d)", data.Len(), d.ExpRows)
			return
		}
		if err == nil && d.ExpErr != "" {
			t.Errorf("Unexpected Success, should have returned error: %s", d.ExpErr)
			return
		}
		if d.ExpCols == nil && data.GetColNames() != nil {
			t.Errorf("Expecting nil columns but got %d of them", data.NumCols())
			return
		}
		if data.NumCols() != len(d.ExpCols) {
			fmt.Println("Expected: ", d.ExpCols)
			fmt.Println("Result: ", data.GetColNames())
			t.Errorf("Number of columns returned (%d) does not match expected number of cols(%d)", data.NumCols(), len(d.ExpCols))
			return
		}
		actCols := data.GetColNames()
		sort.Strings(actCols)
		sort.Strings(d.ExpCols)
		if !reflect.DeepEqual(actCols, d.ExpCols) {
			t.Errorf("Expected Cols (%v) do not match actual cols (%v)", d.ExpCols, actCols)
			return
		}

		if len(data.Vals) == 0 {
			if d.ExpVals == nil || len(d.ExpVals) == 0 {
				return
			}
			t.Errorf("No actual values to test against Expected Vals")
			return
		}
		if d.ExpVals != nil {
			expVals := sqtypes.CreateValuesFromRaw(d.ExpVals)
			msg := sqtypes.Compare2DValue(data.Vals, expVals, "Actual", "Expect")
			if msg != "" {
				t.Error(msg)
				return
			}
		}
	}
}

func TestSelect(t *testing.T) {
	profile := sqprofile.CreateSQProfile()
	// Make sure datasets are by default in RowID order
	sqtables.RowOrder = true

	sqtest.ProcessSQFile("./testdata/selecttests.sq")
	sqtest.ProcessSQFile("./testdata/multitable.sq")

	data := []SelectData{

		{
			TestName: "Select from empty table",
			Command:  "SELECT col1, col2, col3 from selEmpty",
			ExpErr:   "",
			ExpRows:  0,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals:  sqtypes.RawVals{},
		},
		{
			TestName: "SELECT Where invalid",
			Command:  "SELECT col1 FROM seltest WHERE col1=9999999999999999999999",
			ExpErr:   "Error: Type Mismatch: 1E+22 is not an Int",
			ExpRows:  0,
			ExpCols:  []string{"col1"},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT only",
			Command:  "SELECT",
			ExpErr:   "Syntax Error: No columns defined for query",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT missing comma",
			Command:  "SELECT col1",
			ExpErr:   "Syntax Error: Comma is required to separate columns",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT missing FROM",
			Command:  "SELECT col1, col2, col3",
			ExpErr:   "Syntax Error: Comma is required to separate columns",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT missing Table Name",
			Command:  "SELECT col1, col2, col3 FROM",
			ExpErr:   "Syntax Error: No Tables defined for query",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT from seltest",
			Command:  "SELECT col1, col2, col3 FROM seltest",
			ExpErr:   "",
			ExpRows:  3,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
				{789, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT * from seltest",
			Command:  "SELECT * FROM seltest",
			ExpErr:   "",
			ExpRows:  3,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
				{789, "Seltest 3", false},
			},
		},
		{
			TestName: "Invalid table name",
			Command:  "SELECT col1, col2 FROM NotATable",
			ExpErr:   "Error: Table \"NotATable\" does not exist",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "Invalid column name",
			Command:  "SELECT col1, col2, colx FROM seltest",
			ExpErr:   "Error: Column \"colx\" not found in Table(s): seltest",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT * tableName",
			Command:  "SELECT * seltest",
			ExpErr:   "Syntax Error: Expecting FROM",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "Select * from NotATable",
			Command:  "Select * from NotATable",
			ExpErr:   "Error: Table \"NotATable\" does not exist",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT too many columns",
			Command:  "SELECT col1, col2, col3, colx FROM seltest",
			ExpErr:   "Error: Column \"colx\" not found in Table(s): seltest",
			ExpRows:  0,
			ExpCols:  []string{},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT Where",
			Command:  "SELECT col1 FROM seltest WHERE col1=456",
			ExpErr:   "",
			ExpRows:  1,
			ExpCols:  []string{"col1"},
			ExpVals:  sqtypes.RawVals{{456}},
		},
		{
			TestName: "SELECT COUNT",
			Command:  "SELECT COUNT FROM seltest",
			ExpErr:   "Syntax Error: Count must be followed by ()",
			ExpRows:  0,
			ExpCols:  []string{"COUNT"},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT COUNT(",
			Command:  "SELECT COUNT( FROM seltest",
			ExpErr:   "Syntax Error: Count must be followed by ()",
			ExpRows:  0,
			ExpCols:  []string{"COUNT"},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT COUNT)",
			Command:  "SELECT COUNT) FROM seltest",
			ExpErr:   "Syntax Error: Count must be followed by ()",
			ExpRows:  0,
			ExpCols:  []string{"COUNT"},
			ExpVals:  nil,
		},
		{
			TestName: "SELECT COUNT()",
			Command:  "SELECT COUNT() FROM seltest",
			ExpErr:   "",
			ExpRows:  1,
			ExpCols:  []string{"count()"},
			ExpVals:  sqtypes.RawVals{{3}},
		},
		{
			TestName: "SELECT COUNT(), Extra Col",
			Command:  "SELECT COUNT(), id FROM seltest",
			ExpErr:   "Syntax Error: Select Statements with Count() must not have other expressions",
			ExpRows:  1,
			ExpCols:  []string{"count()"},
			ExpVals:  sqtypes.RawVals{{3}},
		},
		{
			TestName: "SELECT Order BY",
			Command:  "SELECT * FROM seltest ORDER BY col1",
			ExpErr:   "",
			ExpRows:  3,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
				{789, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Order BY err",
			Command:  "SELECT * FROM seltest ORDER BY col1, dec",
			ExpErr:   "Error: Column dec not found in dataset",
			ExpRows:  3,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
				{789, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Order BY missing comma",
			Command:  "SELECT * FROM seltest ORDER BY col1 col2",
			ExpErr:   "Syntax Error: Missing comma in ORDER BY clause",
			ExpRows:  3,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
				{789, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Order BY DESC",
			Command:  "SELECT * FROM seltest ORDER BY col1 desc",
			ExpErr:   "",
			ExpRows:  3,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{789, "Seltest 3", false},
				{456, "Seltest 2", true},
				{123, "With Cols Test", true},
			},
		},
		{
			TestName: "SELECT Where & Order BY",
			Command:  "SELECT * FROM seltest WHERE col3 = true ORDER BY col1",
			ExpErr:   "",
			ExpRows:  2,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
			},
		},
		{
			TestName: "SELECT Order BY & Where",
			Command:  "SELECT * FROM seltest ORDER BY col1 WHERE col3 = true ",
			ExpErr:   "",
			ExpRows:  2,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
			},
		},
		{
			TestName: "SELECT Order BY & Where + Extra stuff",
			Command:  "SELECT * FROM seltest ORDER BY col1 WHERE col3 = true extra stuff",
			ExpErr:   "Syntax Error: Unexpected tokens after SQL command:[IDENT=extra] [IDENT=stuff]",
			ExpRows:  2,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
			},
		},
		{
			TestName: "SELECT Order BY + Extra stuff",
			Command:  "SELECT * FROM seltest ORDER BY col1 extra stuff",
			ExpErr:   "Syntax Error: Missing comma in ORDER BY clause",
			ExpRows:  2,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
			},
		},
		{
			TestName: "SELECT + Extra stuff",
			Command:  "SELECT * FROM seltest extra stuff",
			ExpErr:   "Syntax Error: Comma is required to separate tables",
			ExpRows:  2,
			ExpCols:  []string{"col1", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{123, "With Cols Test", true},
				{456, "Seltest 2", true},
			},
		},
		{
			TestName: "SELECT col1*10 ",
			Command:  "SELECT col1*10, col2, col3 FROM seltest",
			ExpErr:   "",
			ExpRows:  3,
			ExpCols:  []string{"(seltest.col1*10)", "col2", "col3"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Double Where ",
			Command:  "SELECT col1 FROM seltest WHERE col1 = 456 Where col1=123",
			ExpErr:   "Syntax Error: Duplicate where clause, only one allowed",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Double Order By ",
			Command:  "SELECT col1 FROM seltest Order by col1 Order by col1 desc",
			ExpErr:   "Syntax Error: Duplicate order by clause, only one allowed",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Where expression err ",
			Command:  "SELECT col1 FROM seltest Where col1<",
			ExpErr:   "Syntax Error: Unexpected end to expression",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Where expression err invalid col ",
			Command:  "SELECT col1 FROM seltest Where colX<5",
			ExpErr:   "Error: Column \"colX\" not found in Table(s): seltest",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Where expression err reduce ",
			Command:  "SELECT col1 FROM seltest Where colX<(5-\"test\")",
			ExpErr:   "Error: Type Mismatch: test is not an Int",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Where count() ",
			Command:  "SELECT col1 FROM seltest Where col1 <count()",
			ExpErr:   "Error: Unable to evaluate \"count()\"",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Where count() ",
			Command:  "SELECT col1 FROM seltest Where col1 <count()",
			ExpErr:   "Error: Unable to evaluate \"count()\"",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Where -count() ",
			Command:  "SELECT col1 FROM seltest Where col1 = -count()",
			ExpErr:   "Error: Unable to evaluate \"count()\"",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Where FLOAT(count()) ",
			Command:  "SELECT col1 FROM seltest Where FLOAT(col1) < FLOAT(count())",
			ExpErr:   "Error: Unable to evaluate \"count()\"",
			ExpRows:  3,
			ExpCols:  []string{"col1"},
			ExpVals: sqtypes.RawVals{
				{1230, "With Cols Test", true},
				{4560, "Seltest 2", true},
				{7890, "Seltest 3", false},
			},
		},
		{
			TestName: "SELECT Join country city ",
			Command:  "SELECT city.name, country.short FROM city, country where city.country = country.name and country.name != \"United States\"",
			ExpErr:   "",
			ExpRows:  6,
			ExpCols:  []string{"city.name", "country.short"},
			ExpVals: sqtypes.RawVals{
				{"Joliette", "CAN"},
				{"Tofino", "CAN"},
				{"Hove", "GBR"},
				{"Leeds", "GBR"},
				{"Manchester", "GBR"},
				{"Sheffield", "GBR"},
			},
		},
		{
			TestName: "SELECT Join country city person ",
			Command:  "select firstname, lastname, city.name, city.prov, country.short from city, country, person where city.country = country.name and country.short!=\"USA\" and city.cityid = person.cityid",
			ExpErr:   "",
			ExpRows:  16,
			ExpCols:  []string{"firstname", "lastname", "city.name", "city.prov", "country.short"},
			ExpVals: sqtypes.RawVals{
				{"Eliana", "Peasel", "Tofino", "British Columbia", "CAN"},
				{"Tyrone", "Ringen", "Tofino", "British Columbia", "CAN"},
				{"Nedra", "Hanaway", "Joliette", "Québec", "CAN"},
				{"Yvone", "June", "Joliette", "Québec", "CAN"},
				{"Grisel", "Martindale", "Joliette", "Québec", "CAN"},
				{"Elva", "Velten", "Joliette", "Québec", "CAN"},
				{"Daron", "Whitcome", "Joliette", "Québec", "CAN"},
				{"Linda", "Calco", "Hove", "Brighton and Hove", "GBR"},
				{"Ocie", "Capossela", "Hove", "Brighton and Hove", "GBR"},
				{"Cornell", "Codilla", "Leeds", "Leeds", "GBR"},
				{"Georgia", "Kuffa", "Leeds", "Leeds", "GBR"},
				{"Jenna", "Merisier", "Leeds", "Leeds", "GBR"},
				{"Sophie", "Schuh", "Leeds", "Leeds", "GBR"},
				{"Rodrigo", "Higman", "Manchester", "Manchester", "GBR"},
				{"Shelton", "Leggat", "Manchester", "Manchester", "GBR"},
				{"Svetlana", "Poirrier", "Sheffield", "Sheffield", "GBR"},
			},
		},
		{
			TestName: "Multi Table Order By with * ",
			Command:  "SELECT * FROM city, country where city.country = country.name and country.name != \"United States\" order by city.name",
			ExpErr:   "",
			ExpRows:  6,
			ExpCols:  []string{"country.name", "country.short", "city.cityid", "city.name", "city.country", "city.prov", "city.lat", "city.long"},
			ExpVals: sqtypes.RawVals{
				{5, "Hove", "United Kingdom", "Brighton and Hove", 50.8333, -0.1833, "United Kingdom", "GBR"},
				{1, "Joliette", "Canada", "Québec", 46.0333, -73.4333, "Canada", "CAN"},
				{3, "Leeds", "United Kingdom", "Leeds", 53.83, -1.58, "United Kingdom", "GBR"},
				{4, "Manchester", "United Kingdom", "Manchester", 53.5004, -2.248, "United Kingdom", "GBR"},
				{2, "Sheffield", "United Kingdom", "Sheffield", 53.3667, -1.5, "United Kingdom", "GBR"},
				{0, "Tofino", "Canada", "British Columbia", 49.1521, -125.9031, "Canada", "CAN"},
			},
		},
		{
			TestName: "Multi Table Order By with alias ",
			Command:  "SELECT city.cityid, city.name cname, lat,long, short  FROM city, country where city.country = country.name and country.name != \"United States\" order by cname",
			ExpErr:   "",
			ExpRows:  6,
			ExpCols:  []string{"city.cityid", "cname", "lat", "long", "short"},
			ExpVals: sqtypes.RawVals{
				{5, "Hove", 50.8333, -0.1833, "GBR"},
				{1, "Joliette", 46.0333, -73.4333, "CAN"},
				{3, "Leeds", 53.83, -1.58, "GBR"},
				{4, "Manchester", 53.5004, -2.248, "GBR"},
				{2, "Sheffield", 53.3667, -1.5, "GBR"},
				{0, "Tofino", 49.1521, -125.9031, "CAN"},
			},
		},

		/*		{
				TestName: "Multi Table Order By with table alias ",
				Command:  "SELECT cn.short,city.cityid, city.name cname, lat,long  FROM city, country cn where city.country = cn.name and cn.name != \"United States\" order by cname",
				ExpErr:   "",
				ExpRows:  6,
				ExpCols:  []string{"city.cityid", "cname", "lat", "long", "cn.short"},
				ExpVals: sqtypes.RawVals{
					{5, "Hove", 50.8333, -0.1833, "GBR"},
					{1, "Joliette", 46.0333, -73.4333, "CAN"},
					{3, "Leeds", 53.83, -1.58, "GBR"},
					{4, "Manchester", 53.5004, -2.248, "GBR"},
					{2, "Sheffield", 53.3667, -1.5, "GBR"},
					{0, "Tofino", 49.1521, -125.9031, "CAN"},
				},
			}*/
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testSelectFunc(profile, row))

	}
}
