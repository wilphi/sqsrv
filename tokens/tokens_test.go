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

type TokenData struct {
	TestName string
	testStr  string
	Tokens   *TokenList
}

func TestTokenize(t *testing.T) {
	data := []TokenData{
		{
			TestName: "Select with extra whitespace",
			testStr: " SElect * from 	Tablea whEre a=b \n",
			Tokens: CreateList([]*Token{Words[Select], SYMBOLS['*'], Words[From], CreateToken(Ident, "Tablea"), Words[Where], CreateToken(Ident, "a"), SYMBOLS['='], CreateToken(Ident, "b")}),
		},
		{
			TestName: "Select with _Identifier",
			testStr:  " SElect * from _Tablea whEre a=b \n",
			Tokens:   CreateList([]*Token{Words[Select], SYMBOLS['*'], Words[From], CreateToken(Ident, "_Tablea"), Words[Where], CreateToken(Ident, "a"), SYMBOLS['='], CreateToken(Ident, "b")}),
		},

		{
			TestName: "Identifier with multiple _",
			testStr:  " SElect * from _Table_a whEre a=b \n",
			Tokens:   CreateList([]*Token{Words[Select], SYMBOLS['*'], Words[From], CreateToken(Ident, "_Table_a"), Words[Where], CreateToken(Ident, "a"), SYMBOLS['='], CreateToken(Ident, "b")}),
		},
		{
			TestName: "Identifier with trailing _",
			testStr:  " SElect * from Tablea whEre a_=b \n",
			Tokens:   CreateList([]*Token{Words[Select], SYMBOLS['*'], Words[From], CreateToken(Ident, "Tablea"), Words[Where], CreateToken(Ident, "a_"), SYMBOLS['='], CreateToken(Ident, "b")}),
		},
		{
			TestName: "Create syntax",
			testStr:  "Create table _tab (col1 int not null, col2 string null, col3 bool, col4 float)",
			Tokens: CreateList([]*Token{Words[Create], Words[Table], CreateToken(Ident, "_tab"), SYMBOLS['('],
				CreateToken(Ident, "col1"), Words[TypeInt], Words[Not], Words[Null], SYMBOLS[','], CreateToken(Ident, "col2"), Words[TypeString], Words[Null],
				SYMBOLS[','], CreateToken(Ident, "col3"), Words[TypeBool], SYMBOLS[','], CreateToken(Ident, "col4"), Words[TypeFloat], SYMBOLS[')']}),
		},
		{
			TestName: "Unknown Char",
			testStr: "SElect * from 	_Tablea whEre a=b ~\n",
			Tokens: CreateList([]*Token{Words[Select], SYMBOLS['*'], Words[From], CreateToken(Ident, "_Tablea"), Words[Where], CreateToken(Ident, "a"), SYMBOLS['='], CreateToken(Ident, "b"), CreateToken(Unk, "~")}),
		},
		{
			TestName: "Number",
			testStr:  "SElect * from Tablea whEre a=1234567890 \n",
			Tokens:   CreateList([]*Token{Words[Select], SYMBOLS['*'], Words[From], CreateToken(Ident, "Tablea"), Words[Where], CreateToken(Ident, "a"), SYMBOLS['='], CreateToken(Num, "1234567890")}),
		},
		{
			TestName: "Negative Number",
			testStr:  "SElect * from Tablea whEre a=-1234567890 \n",
			Tokens:   CreateList([]*Token{Words[Select], SYMBOLS['*'], Words[From], CreateToken(Ident, "Tablea"), Words[Where], CreateToken(Ident, "a"), SYMBOLS['='], CreateToken(Num, "-1234567890")}),
		},
		{
			TestName: "Decimal Number",
			testStr:  "SElect * from Tablea whEre a=123.4567890 \n",
			Tokens:   CreateList([]*Token{Words[Select], SYMBOLS['*'], Words[From], CreateToken(Ident, "Tablea"), Words[Where], CreateToken(Ident, "a"), SYMBOLS['='], CreateToken(Num, "123.4567890")}),
		},

		{
			TestName: "Quoted string",
			testStr:  "Insert Into test1 (col1, col2, col3) values (123, \"test str\", true);",
			Tokens: CreateList([]*Token{Words[Insert], Words[Into], CreateToken(Ident, "test1"),
				SYMBOLS['('], CreateToken(Ident, "col1"), SYMBOLS[','], CreateToken(Ident, "col2"), SYMBOLS[','], CreateToken(Ident, "col3"), SYMBOLS[')'],
				Words[Values], SYMBOLS['('], CreateToken(Num, "123"), SYMBOLS[','], CreateToken(Quote, "test str"), SYMBOLS[','], Words[RWTrue], SYMBOLS[')'], SYMBOLS[';']}),
		},
		{
			TestName: "Missing End Quote for string",
			testStr:  "Insert Into test1 (col1, col2, col3) values (123, \"test str, true);",
			Tokens: CreateList([]*Token{Words[Insert], Words[Into], CreateToken(Ident, "test1"),
				SYMBOLS['('], CreateToken(Ident, "col1"), SYMBOLS[','], CreateToken(Ident, "col2"), SYMBOLS[','], CreateToken(Ident, "col3"), SYMBOLS[')'],
				Words[Values], SYMBOLS['('], CreateToken(Num, "123"), SYMBOLS[','], CreateToken(Err, "Missing End Quote")}),
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testTokenizeFunc(row))

	}

}

func TestMiscFunctions(t *testing.T) {

	t.Run("(*Token)GetString() tests", func(t *testing.T) {
		for name, tkn := range Words {
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
		if tkns.ToString() != d.Tokens.ToString() {
			t.Errorf("Token list %q does not match expected list %q", tkns.ToString(), d.Tokens.ToString())
		}
	}
}
