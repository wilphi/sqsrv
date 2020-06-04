package cmd_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/tokens"
)

///////////////////////////////////////////////////////////////////////////////////////////

type ParseFromClauseData struct {
	TestName       string
	Terminators    []tokens.TokenID
	Command        string
	ExpErr         string
	ExpTab         *sqtables.TableList
	ExpectedTables []*moniker.Moniker
	ExpTokenLen    int
}

func testParseFromClauseFunc(d ParseFromClauseData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		profile := sqprofile.CreateSQProfile()

		tkns := tokens.Tokenize(d.Command)

		rIdents, _, err := cmd.ParseFromClause(profile, tkns, d.Terminators...)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if tkns.Len() != d.ExpTokenLen {
			t.Error("All tokens should be consumed by test")
			return
		}
		if len(d.ExpectedTables) != rIdents.Len() {
			t.Errorf("The length Expected TableNames (%d) and returned TableNames (%d) do not match", len(d.ExpectedTables), rIdents.Len())
			return
		}

		sort.Slice(d.ExpectedTables, func(i int, j int) bool { return d.ExpectedTables[i].Name() < d.ExpectedTables[j].Name() })
		if !reflect.DeepEqual(d.ExpectedTables, rIdents.TableNames()) {
			t.Errorf("Expected Tables: %v  - do not match actual Tables: %v", d.ExpectedTables, rIdents.TableNames())
			return
		}

	}
}
func TestParseFromClause(t *testing.T) {
	////////////////////////////////////////////
	// Setup tables for tests
	////////////////////////////////////////////
	profile := sqprofile.CreateSQProfile()
	tableData := []struct {
		Name string
		Col  []column.Def
	}{
		{Name: "parsefromTable1", Col: []column.Def{column.NewDef("col1", tokens.Int, false)}},
		{Name: "parsefromTable2", Col: []column.Def{column.NewDef("col1", tokens.Int, false)}},
		{Name: "parsefromTable3", Col: []column.Def{column.NewDef("col1", tokens.Int, false)}},
		{Name: "parsefromcountry", Col: []column.Def{column.NewDef("col1", tokens.Int, false), column.NewDef("countryid", tokens.Int, false)}},
		{Name: "parsefromcity", Col: []column.Def{column.NewDef("col1", tokens.Int, false), column.NewDef("countryid", tokens.Int, false), column.NewDef("cityid", tokens.Int, false)}},
		{Name: "parsefromperson", Col: []column.Def{column.NewDef("col1", tokens.Int, false), column.NewDef("cityid", tokens.Int, false)}},
	}
	for _, tabDat := range tableData {
		tab := sqtables.CreateTableDef(tabDat.Name, tabDat.Col...)
		err := sqtables.CreateTable(profile, tab)
		if err != nil {
			t.Errorf("Error setting up %s: %s", t.Name(), tabDat.Name)
			return
		}
	}

	data := []ParseFromClauseData{

		{
			TestName:       "One Table",
			Terminators:    []tokens.TokenID{tokens.CloseBracket},
			Command:        "parsefromTable1",
			ExpErr:         "",
			ExpectedTables: []*moniker.Moniker{moniker.New("parsefromtable1", "")},
		},
		{
			TestName:       "One Table Invalid",
			Terminators:    []tokens.TokenID{tokens.CloseBracket},
			Command:        "NotATable",
			ExpErr:         "Error: Table \"notatable\" does not exist",
			ExpectedTables: []*moniker.Moniker{},
		},
		{
			TestName:       "One Table with alias",
			Terminators:    []tokens.TokenID{tokens.CloseBracket},
			Command:        "parsefromTable1 t1",
			ExpErr:         "",
			ExpectedTables: []*moniker.Moniker{moniker.New("parsefromtable1", "t1")},
		},
		{
			TestName:       "Expect another Table",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order},
			Command:        "parsefromTable1,",
			ExpErr:         "Syntax Error: Unexpected ',' in From clause",
			ExpectedTables: nil,
		},
		{
			TestName:       "Two Tables",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order},
			Command:        "parsefromTable1, parsefromTable2",
			ExpErr:         "Syntax Error: Unexpected ',' in From clause",
			ExpectedTables: []*moniker.Moniker{},
		},
		{
			TestName:       "No Tables in list",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order},
			Command:        "Where",
			ExpErr:         "Syntax Error: No Tables defined for query",
			ExpectedTables: []*moniker.Moniker{},
		},
		{
			TestName:       "Not a tablename",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order},
			Command:        "() Where",
			ExpErr:         "Syntax Error: Expecting name of Table",
			ExpectedTables: []*moniker.Moniker{},
		},
		{
			TestName:       "Missing tablename",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order},
			Command:        ", test Where",
			ExpErr:         "Syntax Error: Expecting name of Table",
			ExpectedTables: []*moniker.Moniker{},
		},

		{
			TestName:       "Missing Join",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order},
			Command:        "parsefromTable1  alias1 gettablelistTable2 Where",
			ExpErr:         "Syntax Error: Missing Join in From clause near \"gettablelistTable2\"",
			ExpectedTables: []*moniker.Moniker{},
		},

		{
			TestName:       "Unexpected token",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INSERT parsefromcity ON parsefromcountry.countryid = parsefromcity.countryid",
			ExpErr:         "Syntax Error: Unexpected end of From clause at INSERT",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Inner Join",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INNER JOIN parsefromcity ON parsefromcountry.countryid = parsefromcity.countryid",
			ExpErr:         "",
			ExpectedTables: []*moniker.Moniker{moniker.New("parsefromcountry", ""), moniker.New("parsefromcity", "")},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Inner no Join",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INNER parsefromcity ON parsefromcountry.countryid = parsefromcity.countryid",
			ExpErr:         "Syntax Error: Expecting JOIN keyword after INNER",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Inner Join No On",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INNER JOIN parsefromcity",
			ExpErr:         "Syntax Error: Expecting keyword ON after second table name in a JOIN",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Inner Join partial ON",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INNER JOIN parsefromcity ON parsefromcountry.countryid = ",
			ExpErr:         "Syntax Error: Unexpected end to expression",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Inner Join ON col only",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INNER JOIN parsefromcity ON parsefromcity.countryid",
			ExpErr:         "Syntax Error: ON clause must take the form of <tablea>.<colx> = <tableb>.<coly>",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Inner Join ON multicol ",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INNER JOIN parsefromcity ON parsefromcountry.countryid = parsefromcity.countryid AND parsefromcountry.col1 = parsefromcity.col1",
			ExpErr:         "Error: Multi column joins are not currently implemented",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Inner Join ON value to left",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INNER JOIN parsefromcity ON 1 = parsefromcity.countryid",
			ExpErr:         "Syntax Error: Expression to the left of = in ON clause is not a column",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Inner Join same table",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcity INNER JOIN parsefromcountry ON parsefromcountry.countryid = parsefromcountry.countryid",
			ExpErr:         "Syntax Error: To join tables, the ON clause must reference at least two different ones",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},

		{
			TestName:       "Inner Join same table with aliases",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry c1 INNER JOIN parsefromcountry c2 ON c1.countryid = c2.countryid",
			ExpErr:         "",
			ExpectedTables: []*moniker.Moniker{moniker.New("parsefromcountry", "c1"), moniker.New("parsefromcountry", "c2")},
			ExpTokenLen:    0,
		},
		{
			TestName:       "CROSS Join",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry CROSS JOIN parsefromcity",
			ExpErr:         "",
			ExpectedTables: []*moniker.Moniker{moniker.New("parsefromcountry", ""), moniker.New("parsefromcity", "")},
			ExpTokenLen:    0,
		},
		{
			TestName:       "CROSS Join no second table",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry CROSS JOIN",
			ExpErr:         "Syntax Error: Expecting a tablename after JOIN",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "CROSS Join invalid second table",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry CROSS JOIN notatable",
			ExpErr:         "Error: Table \"notatable\" does not exist",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "CROSS Join with ON",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry CROSS JOIN parsefromcity ON parsefromcountry.countryid = parsefromcity.countryid",
			ExpErr:         "Syntax Error: Cross joins must not have an ON expression",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		}, {
			TestName:       "Join only",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry JOIN parsefromcity ON parsefromcountry.countryid = parsefromcity.countryid",
			ExpErr:         "",
			ExpectedTables: []*moniker.Moniker{moniker.New("parsefromcountry", ""), moniker.New("parsefromcity", "")},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Outer Join only",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry OUTER JOIN parsefromcity ON parsefromcountry.countryid = parsefromcity.countryid",
			ExpErr:         "Syntax Error: LEFT or RIGHT keyword required before OUTER JOIN",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Left Join only",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry Left JOIN parsefromcity ON parsefromcountry.countryid = parsefromcity.countryid",
			ExpErr:         "Syntax Error: Expecting OUTER keyword after join type LEFT",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Left Outer Join ",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry Left OUTER JOIN parsefromcity ON parsefromcountry.countryid = parsefromcity.countryid",
			ExpErr:         "",
			ExpectedTables: []*moniker.Moniker{moniker.New("parsefromcountry", ""), moniker.New("parsefromcity", "")},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Right Join only",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry Right JOIN parsefromcity ON parsefromcountry.countryid = parsefromcity.countryid",
			ExpErr:         "Syntax Error: Expecting OUTER keyword after join type RIGHT",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Inner Join Invalid on col",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INNER JOIN parsefromcity ON parsefromcountry.notacol = parsefromcity.countryid",
			ExpErr:         "Error: Column \"notacol\" not found in Table \"parsefromcountry\"",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Inner Join Invalid on table",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INNER JOIN parsefromcity ON notatable.countryid = parsefromcity.countryid",
			ExpErr:         "Error: Table notatable not found in table list",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Double Inner Join",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INNER JOIN parsefromcity ON parsefromcountry.countryid = parsefromcity.countryid INNER JOIN parsefromperson ON parsefromperson.cityid = parsefromcity.cityid",
			ExpErr:         "",
			ExpectedTables: []*moniker.Moniker{moniker.New("parsefromcountry", ""), moniker.New("parsefromcity", ""), moniker.New("parsefromperson", "")},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Double Inner Join Invalid On",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INNER JOIN parsefromcity ON parsefromcountry.countryid = parsefromcity.countryid INNER JOIN parsefromperson ON parsefromcountry.countryid = parsefromcity.countryid",
			ExpErr:         "Syntax Error: The table parsefromperson must be used as a join condition in the ON statement",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Inner Join Invalid on with value",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INNER JOIN parsefromcity ON parsefromcountry.countryid = 5",
			ExpErr:         "Syntax Error: The table parsefromcity must be used as a join condition in the ON statement",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		},
		{
			TestName:       "Inner Join Invalid on with value v2",
			Terminators:    []tokens.TokenID{tokens.Where, tokens.Order, tokens.Group, tokens.Having},
			Command:        "parsefromcountry INNER JOIN parsefromcity ON parsefromcity.countryid = 5",
			ExpErr:         "Syntax Error: Expression to the right of = in ON clause is not a column",
			ExpectedTables: []*moniker.Moniker{},
			ExpTokenLen:    0,
		}}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testParseFromClauseFunc(row))

	}

}
