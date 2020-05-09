package tokens

import (
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/wilphi/sqsrv/sqtest"
)

func TestMain(m *testing.M) {

	sqtest.TestInit("tokens_test.log")
	os.Exit(m.Run())

}

type TokenData struct {
	TestName string
	testStr  string
	Tokens   *TokenList
}

func allWords() []Token {
	var tkns []Token

	for _, tkn := range wordTokens {
		if tkn.TestFlags(IsWord) {
			tkns = append(tkns, tkn)
		}
	}
	sort.Slice(tkns, func(i, j int) bool { return tkns[i].Name() < tkns[j].Name() })
	return tkns
}

func allFunctions() []Token {
	var tkns []Token

	for _, tkn := range wordTokens {
		if tkn.TestFlags(IsFunction) {
			tkns = append(tkns, tkn)
		}
	}
	sort.Slice(tkns, func(i, j int) bool { return tkns[i].Name() < tkns[j].Name() })

	return tkns
}
func TestTokenize(t *testing.T) {
	data := []TokenData{
		{
			TestName: "Select with extra whitespace",
			testStr: " SElect * from 	Tablea whEre a=b \n",
			Tokens: CreateList([]Token{GetWordToken(Select), GetWordToken(Asterix), GetWordToken(From), NewValueToken(Ident, "Tablea"), GetWordToken(Where), NewValueToken(Ident, "a"), GetWordToken(Equal), NewValueToken(Ident, "b")}),
		},
		{
			TestName: "Select with _Identifier",
			testStr:  " SElect * from _Tablea whEre a=b \n",
			Tokens:   CreateList([]Token{GetWordToken(Select), GetWordToken(Asterix), GetWordToken(From), NewValueToken(Ident, "_Tablea"), GetWordToken(Where), NewValueToken(Ident, "a"), GetWordToken(Equal), NewValueToken(Ident, "b")}),
		},

		{
			TestName: "Identifier with multiple _",
			testStr:  " SElect * from _Table_a whEre a=b \n",
			Tokens:   CreateList([]Token{GetWordToken(Select), GetWordToken(Asterix), GetWordToken(From), NewValueToken(Ident, "_Table_a"), GetWordToken(Where), NewValueToken(Ident, "a"), GetWordToken(Equal), NewValueToken(Ident, "b")}),
		},
		{
			TestName: "Identifier with trailing _",
			testStr:  " SElect * from Tablea whEre a_=b \n",
			Tokens:   CreateList([]Token{GetWordToken(Select), GetWordToken(Asterix), GetWordToken(From), NewValueToken(Ident, "Tablea"), GetWordToken(Where), NewValueToken(Ident, "a_"), GetWordToken(Equal), NewValueToken(Ident, "b")}),
		},
		{
			TestName: "Create syntax",
			testStr:  "Create table _tab (col1 int not null, col2 string null, col3 bool, col4 float)",
			Tokens: CreateList([]Token{GetWordToken(Create), GetWordToken(Table), NewValueToken(Ident, "_tab"), GetWordToken(OpenBracket),
				NewValueToken(Ident, "col1"), GetWordToken(Int), GetWordToken(Not), GetWordToken(Null), GetWordToken(Comma), NewValueToken(Ident, "col2"), GetWordToken(String), GetWordToken(Null),
				GetWordToken(Comma), NewValueToken(Ident, "col3"), GetWordToken(Bool), GetWordToken(Comma), NewValueToken(Ident, "col4"), GetWordToken(Float), GetWordToken(CloseBracket)}),
		},
		{
			TestName: "Unknown Char",
			testStr: "SElect * from 	_Tablea whEre a=b ~\n",
			Tokens: CreateList([]Token{GetWordToken(Select), GetWordToken(Asterix), GetWordToken(From), NewValueToken(Ident, "_Tablea"), GetWordToken(Where), NewValueToken(Ident, "a"), GetWordToken(Equal), NewValueToken(Ident, "b"), NewValueToken(Unk, "~")}),
		},
		{
			TestName: "Number",
			testStr:  "SElect * from Tablea whEre a=1234567890 \n",
			Tokens:   CreateList([]Token{GetWordToken(Select), GetWordToken(Asterix), GetWordToken(From), NewValueToken(Ident, "Tablea"), GetWordToken(Where), NewValueToken(Ident, "a"), GetWordToken(Equal), NewValueToken(Num, "1234567890")}),
		},
		{
			TestName: "Negative Number",
			testStr:  "SElect * from Tablea whEre a=-1234567890 \n",
			Tokens:   CreateList([]Token{GetWordToken(Select), GetWordToken(Asterix), GetWordToken(From), NewValueToken(Ident, "Tablea"), GetWordToken(Where), NewValueToken(Ident, "a"), GetWordToken(Equal), GetWordToken(Minus), NewValueToken(Num, "1234567890")}),
		},
		{
			TestName: "Decimal Number",
			testStr:  "SElect * from Tablea whEre a=123.4567890 \n",
			Tokens:   CreateList([]Token{GetWordToken(Select), GetWordToken(Asterix), GetWordToken(From), NewValueToken(Ident, "Tablea"), GetWordToken(Where), NewValueToken(Ident, "a"), GetWordToken(Equal), NewValueToken(Num, "123.4567890")}),
		},

		{
			TestName: "Quoted string",
			testStr:  "Insert Into test1 (col1, col2, col3) values (123, \"test str\", true);",
			Tokens: CreateList([]Token{GetWordToken(Insert), GetWordToken(Into), NewValueToken(Ident, "test1"),
				GetWordToken(OpenBracket), NewValueToken(Ident, "col1"), GetWordToken(Comma), NewValueToken(Ident, "col2"), GetWordToken(Comma), NewValueToken(Ident, "col3"), GetWordToken(CloseBracket),
				GetWordToken(Values), GetWordToken(OpenBracket), NewValueToken(Num, "123"), GetWordToken(Comma), NewValueToken(Quote, "test str"), GetWordToken(Comma), GetWordToken(RWTrue), GetWordToken(CloseBracket), GetWordToken(SemiColon)}),
		},
		{
			TestName: "Missing End Quote for string",
			testStr:  "Insert Into test1 (col1, col2, col3) values (123, \"test str, true);",
			Tokens: CreateList([]Token{GetWordToken(Insert), GetWordToken(Into), NewValueToken(Ident, "test1"),
				GetWordToken(OpenBracket), NewValueToken(Ident, "col1"), GetWordToken(Comma), NewValueToken(Ident, "col2"), GetWordToken(Comma), NewValueToken(Ident, "col3"), GetWordToken(CloseBracket),
				GetWordToken(Values), GetWordToken(OpenBracket), NewValueToken(Num, "123"), GetWordToken(Comma), NewValueToken(Err, "Missing End Quote")}),
		},
		{
			TestName: "Multi char Symbol ",
			testStr:  " SElect * from _Table_a whEre a<=b \n",
			Tokens:   CreateList([]Token{GetWordToken(Select), GetWordToken(Asterix), GetWordToken(From), NewValueToken(Ident, "_Table_a"), GetWordToken(Where), NewValueToken(Ident, "a"), GetWordToken(LessThanEqual), NewValueToken(Ident, "b")}),
		},
		{
			TestName: "All WordTokens ",
			testStr:  "AND ASC AVG BOOL BY COUNT CREATE DELETE DESC DISTINCT DROP FALSE FLOAT FROM FULL GROUP HAVING INNER INSERT INT INTO JOIN LEFT MAX MIN NOT NULL ON OR ORDER OUTER RIGHT SELECT SET STRING SUM TABLE TRUE UPDATE VALUES WHERE \n",
			Tokens:   CreateList(allWords()),
		},
		{
			TestName: "All Functions ",
			testStr:  "AVG BOOL COUNT FLOAT INT MAX MIN STRING SUM\n",
			Tokens:   CreateList(allFunctions()),
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testTokenizeFunc(row))

	}

}

func TestMiscFunctions(t *testing.T) {

	t.Run("(Token)String() tests", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		for i, tkn := range wordTokens {
			if wordNames[i] != tkn.String() {
				t.Errorf("%s != tkn.String() -> %s", wordNames[i], tkn.String())
			}
		}

		// test ident
		tkn := NewValueToken(Ident, "test")
		expected := "[IDENT=test]"
		if tkn.String() != expected {
			t.Errorf("%s != tkn.String() -> %s", expected, tkn.String())
		}
		tkn = NewValueToken(Unk, "test")
		expected = "[UNK=test]"
		if tkn.String() != expected {
			t.Errorf("%s != tkn.String() -> %s", expected, tkn.String())
		}
	})
	t.Run("(Token)Name() tests", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		tkn := NewValueToken(Unk, "test")
		expected := "[UNK=test]"
		if tkn.Name() != expected {
			t.Errorf("%s != tkn.Name() -> %s", expected, tkn.Name())
		}
	})
	t.Run("(Token)SetValue tests", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		tkn := NewValueToken(Unk, "test")
		vtkn := tkn.(*ValueToken)
		expected := "UNK"
		vtkn.SetValue(expected)
		if vtkn.Value() != expected {
			t.Errorf("tkn.SetValue Failed! Expected %s != tkn.Value() -> %s", expected, vtkn.Value())
		}
	})

}

func testTokenizeFunc(d TokenData) func(t *testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		tkns := Tokenize(d.testStr)
		if tkns.String() != d.Tokens.String() {
			t.Errorf("Token list %q does not match expected list %q", tkns.String(), d.Tokens.String())
		}
	}
}
