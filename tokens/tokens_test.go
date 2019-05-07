package tokens

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// setup logging
	logFile, err := os.OpenFile("sqparser_test.log", os.O_CREATE|os.O_WRONLY, 0666)
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
func TestGetTokens(t *testing.T) {
	names := []string{Select, Asterix, From, Ident, Where, Ident, Equal, Ident}
	testStr := "SElect * from 	Tablea whEre a=b \n"
	t.Run(testStr, testTokenizeFunc(testStr, names))

	testStr = "SElect * from 	_Tablea whEre a=b \n"
	t.Run(testStr, testTokenizeFunc(testStr, names))

	//only leading _ is allow for Ident
	names = []string{Select, Asterix, From, Ident, Where, Ident, Equal, Ident}
	testStr = "SElect * from 	_Table_a whEre a=b \n"
	t.Run(testStr, testTokenizeFunc(testStr, names))

	//only leading _ is allow for Ident
	names = []string{Select, Asterix, From, Ident, Where, Ident, Equal, Ident}
	testStr = "SElect * from 	_Tablea whEre a_=b \n"
	t.Run(testStr, testTokenizeFunc(testStr, names))

	names = []string{Create, Table, Ident, OpenBracket,
		Ident, Colon, TypeTKN, Comma, Ident, Colon, TypeTKN,
		Comma, Ident, Colon, TypeTKN, CloseBracket}
	testStr = "Create table _tab (col1:int, col2:string, col3:bool)"
	t.Run(testStr, testTokenizeFunc(testStr, names))

	names = []string{Select, Asterix, From, Ident, Where, Ident, Equal, Ident, Unk}
	testStr = "SElect * from 	_Tablea whEre a=b ~\n"
	t.Run(testStr, testTokenizeFunc(testStr, names))

}

func TestInsertStatment(t *testing.T) {
	testStr := "Insert Into test1 (col1, col2, col3) values (123, \"test str\", true);"
	names := []string{Insert, Into, Ident,
		OpenBracket, Ident, Comma, Ident, Comma, Ident, CloseBracket,
		Values, OpenBracket, Num, Comma, Quote, Comma, RWTrue, CloseBracket, SemiColon}
	t.Run(testStr, testTokenizeFunc(testStr, names))

	// test with no end quote
	testStr = "Insert Into test1 (col1, col2, col3) values (123, \"test str, true);"
	names = []string{Insert, Into, Ident,
		OpenBracket, Ident, Comma, Ident, Comma, Ident, CloseBracket,
		Values, OpenBracket, Num, Comma, Err}
	t.Run(testStr, testTokenizeFunc(testStr, names))

}

func TestMiscFunctions(t *testing.T) {

	t.Run("(*Token)GetString() tests", func(*testing.T) {
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
	t.Run("(*Token)GetName() tests", func(*testing.T) {
		tkn := CreateToken(Unk, "test")
		expected := "UNK"
		if tkn.GetName() != expected {
			t.Error(fmt.Sprintf("%s != tkn.GetName() -> %s", expected, tkn.GetName()))
		}
	})
	t.Run("(*Token)SetValue tests", func(*testing.T) {
		tkn := CreateToken(Unk, "test")
		expected := "UNK"
		tkn.SetValue(expected)
		if tkn.GetValue() != expected {
			t.Error(fmt.Sprintf("tkn.SetValue Failed! Expected %s != tkn.GetValue() -> %s", expected, tkn.GetValue()))
		}
	})
	/*
		t.Run("TestToken tests", func(*testing.T) {
			if ret := TestToken([]Token{}, Select, From); ret != "" {
				t.Error(fmt.Sprintf("TestToken Failed! Expected empty string but got -> %s", ret))
			}
			if ret := TestToken([]Token{AllWordTokens[Select]}, Select, From); ret == "" {
				t.Error(fmt.Sprintf("TestToken Failed! Expected %s but got -> %s", Select, ret))
			}
		})
		t.Run("TokensToString tests", func(*testing.T) {
			tkns := []Token{AllWordTokens[Select],
				SYMBOLS['*'],
				AllWordTokens[From],
				CreateToken(Ident, "seltest"),
			}
			expected := "SELECT * FROM [IDENT=seltest]"
			if TokensToString(tkns) != expected {
				t.Error(fmt.Sprintf("Expected: %q, got %q", expected, TokensToString(tkns)))
			}

		})
	*/
}

func testTokenizeFunc(testStr string, names []string) func(*testing.T) {
	return func(t *testing.T) {
		tkns := Tokenize(testStr)
		if !matchNames(names, tkns) {
			t.Error(fmt.Sprint("Token list does not match expected list"))
		}
	}
}
