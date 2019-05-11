package tokens

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// setup logging
	logFile, err := os.OpenFile("tokens_test.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)

	os.Exit(m.Run())

}

//match tokenList
func matchNames(names []string, tkns *TokenList) bool {
	if len(names) != tkns.Len() {
		return false
	}
	for i := 0; i < len(names); i++ {
		if names[i] != tkns.Peek().tokenID {
			return false
		}
		tkns.Remove()
	}
	return true
}

type TokenData struct {
	TestName string
	testStr  string
	names    []string
}

func TestTokenize(t *testing.T) {
	data := []TokenData{
		{
			TestName: "Select with extra whitespace",
			testStr: " SElect * from 	Tablea whEre a=b \n",
			names: []string{Select, Asterix, From, Ident, Where, Ident, Equal, Ident},
		},
		{
			TestName: "Select with _Identifier",
			testStr:  " SElect * from _Tablea whEre a=b \n",
			names:    []string{Select, Asterix, From, Ident, Where, Ident, Equal, Ident},
		},
		{
			TestName: "Identifier with multiple _",
			testStr:  " SElect * from _Table_a whEre a=b \n",
			names:    []string{Select, Asterix, From, Ident, Where, Ident, Equal, Ident},
		},
		{
			TestName: "Identifier with trailing _",
			testStr:  " SElect * from Tablea whEre a_=b \n",
			names:    []string{Select, Asterix, From, Ident, Where, Ident, Equal, Ident},
		},
		{
			TestName: "Create syntax",
			testStr:  "Create table _tab (col1 int not null, col2 string null, col3 bool)",
			names: []string{Create, Table, Ident, OpenBracket,
				Ident, TypeTKN, Not, Null, Comma, Ident, TypeTKN, Null,
				Comma, Ident, TypeTKN, CloseBracket},
		},
		{
			TestName: "Unknown Char",
			testStr: "SElect * from 	_Tablea whEre a=b ~\n",
			names: []string{Select, Asterix, From, Ident, Where, Ident, Equal, Ident, Unk},
		},
		{
			TestName: "Quoted string",
			testStr:  "Insert Into test1 (col1, col2, col3) values (123, \"test str\", true);",
			names: []string{Insert, Into, Ident,
				OpenBracket, Ident, Comma, Ident, Comma, Ident, CloseBracket,
				Values, OpenBracket, Num, Comma, Quote, Comma, RWTrue, CloseBracket, SemiColon},
		},
		{
			TestName: "Missing End Quote for string",
			testStr:  "Insert Into test1 (col1, col2, col3) values (123, \"test str, true);",
			names: []string{Insert, Into, Ident,
				OpenBracket, Ident, Comma, Ident, Comma, Ident, CloseBracket,
				Values, OpenBracket, Num, Comma, Err},
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testTokenizeFunc(row))

	}

}

func TestMiscFunctions(t *testing.T) {

	t.Run("(*Token)GetString() tests", func(t *testing.T) {
		for name, tkn := range AllWordTokens {
			if name != tkn.GetString() {
				t.Error(fmt.Sprintf("%s != tkn.GetString() -> %s", name, tkn.GetString()))
			}
		}
		for s, tkn := range SYMBOLS {
			if string(s) != tkn.GetString() {
				t.Error(fmt.Sprintf("%s != tkn.GetString() -> %s", string(s), tkn.GetString()))
			}
		}

		// test ident
		tkn := CreateToken(Ident, "test")
		expected := "[IDENT=test]"
		if tkn.GetString() != expected {
			t.Error(fmt.Sprintf("%s != tkn.GetString() -> %s", expected, tkn.GetString()))
		}
		tkn = CreateToken(Unk, "test")
		expected = "[UNK, test]"
		if tkn.GetString() != expected {
			t.Error(fmt.Sprintf("%s != tkn.GetString() -> %s", expected, tkn.GetString()))
		}
	})
	t.Run("(*Token)GetName() tests", func(t *testing.T) {
		tkn := CreateToken(Unk, "test")
		expected := "UNK"
		if tkn.GetName() != expected {
			t.Error(fmt.Sprintf("%s != tkn.GetName() -> %s", expected, tkn.GetName()))
		}
	})
	t.Run("(*Token)SetValue tests", func(t *testing.T) {
		tkn := CreateToken(Unk, "test")
		expected := "UNK"
		tkn.SetValue(expected)
		if tkn.GetValue() != expected {
			t.Error(fmt.Sprintf("tkn.SetValue Failed! Expected %s != tkn.GetValue() -> %s", expected, tkn.GetValue()))
		}
	})

}

func testTokenizeFunc(d TokenData) func(t *testing.T) {
	return func(t *testing.T) {
		tkns := Tokenize(d.testStr)
		if !matchNames(d.names, tkns) {
			t.Error(fmt.Sprint("Token list does not match expected list"))
		}
	}
}
