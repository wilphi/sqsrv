package tokens

import (
	"strings"

	log "github.com/sirupsen/logrus"
)

// Token - Individual token for parser
type Token struct {
	tokenID    string
	tokenValue string
}

// Token Constants
const (
	Err          = "ERR"
	Unk          = "UNK"
	Ws           = "WS"
	Ident        = "IDENT"
	Quote        = "QUOTE"
	Num          = "NUM"
	Asterix      = "ASTERIX"
	Period       = "PERIOD"
	Equal        = "EQUAL"
	OpenBracket  = "OPENBRACKET"
	CloseBracket = "CLOSEBRACKET"
	Comma        = "COMMA"
	Colon        = "COLON"
	UnderScore   = "UnderScore"
	SemiColon    = "SemiColon"
	LessThan     = "LessThan"
	GreaterThan  = "GreaterThan"
	Minus        = "Minus"
)

// SYMBOLS - tokens that are individual runes
var SYMBOLS = map[rune]*Token{
	'*': &Token{tokenID: Asterix, tokenValue: "*"},
	'.': &Token{tokenID: Period, tokenValue: "."},
	'=': &Token{tokenID: Equal, tokenValue: "="},
	'(': &Token{tokenID: OpenBracket, tokenValue: "("},
	')': &Token{tokenID: CloseBracket, tokenValue: ")"},
	',': &Token{tokenID: Comma, tokenValue: ","},
	':': &Token{tokenID: Colon, tokenValue: ":"},
	'_': &Token{tokenID: UnderScore, tokenValue: "_"},
	';': &Token{tokenID: SemiColon, tokenValue: ";"},
	'<': &Token{tokenID: LessThan, tokenValue: "<"},
	'>': &Token{tokenID: GreaterThan, tokenValue: ">"},
	'-': &Token{tokenID: Minus, tokenValue: "-"},
}

// Reserved Words
const (
	Create  = "CREATE"
	Table   = "TABLE"
	Select  = "SELECT"
	From    = "FROM"
	Where   = "WHERE"
	And     = "AND"
	Insert  = "INSERT"
	Into    = "INTO"
	Values  = "VALUES"
	RWTrue  = "TRUE"
	RWFalse = "FALSE"
	Not     = "NOT"
	Or      = "OR"
	Delete  = "DELETE"
	Count   = "COUNT"
	Null    = "NULL"
	Drop    = "DROP"
	Update  = "UPDATE"
	Set     = "SET"
	Order   = "ORDER"
	By      = "BY"
	Asc     = "ASC"
	Desc    = "DESC"
)

// Types
const (
	TypeTKN    = "TYPETOKEN"
	TypeInt    = "INT"
	TypeString = "STRING"
	TypeBool   = "BOOL"
	TypeFloat  = "FLOAT"
)

//Type Lengths
const (
	LenInt   = 10
	LenStr   = 20
	LenBool  = 5
	LenFloat = 20
)

var reservedTypes = []string{TypeInt, TypeString, TypeBool, TypeFloat}

// Words - All Reserved Workds and Types as tokens
var Words = map[string]*Token{
	Create:     &Token{tokenID: Create, tokenValue: Create},
	Table:      &Token{tokenID: Table, tokenValue: Table},
	Select:     &Token{tokenID: Select, tokenValue: Select},
	From:       &Token{tokenID: From, tokenValue: From},
	Where:      &Token{tokenID: Where, tokenValue: Where},
	And:        &Token{tokenID: And, tokenValue: And},
	Insert:     &Token{tokenID: Insert, tokenValue: Insert},
	Into:       &Token{tokenID: Into, tokenValue: Into},
	Values:     &Token{tokenID: Values, tokenValue: Values},
	RWTrue:     &Token{tokenID: RWTrue, tokenValue: RWTrue},
	RWFalse:    &Token{tokenID: RWFalse, tokenValue: RWFalse},
	TypeInt:    &Token{tokenID: TypeTKN, tokenValue: TypeInt},
	TypeString: &Token{tokenID: TypeTKN, tokenValue: TypeString},
	TypeBool:   &Token{tokenID: TypeTKN, tokenValue: TypeBool},
	TypeFloat:  &Token{tokenID: TypeTKN, tokenValue: TypeFloat},
	Not:        &Token{tokenID: Not, tokenValue: Not},
	Or:         &Token{tokenID: Or, tokenValue: Or},
	Delete:     &Token{tokenID: Delete, tokenValue: Delete},
	Count:      &Token{tokenID: Count, tokenValue: Count},
	Null:       &Token{tokenID: Null, tokenValue: Null},
	Drop:       &Token{tokenID: Drop, tokenValue: Drop},
	Update:     &Token{tokenID: Update, tokenValue: Update},
	Set:        &Token{tokenID: Set, tokenValue: Set},
	Order:      &Token{tokenID: Order, tokenValue: Order},
	By:         &Token{tokenID: By, tokenValue: By},
	Asc:        &Token{tokenID: Asc, tokenValue: Asc},
	Desc:       &Token{tokenID: Desc, tokenValue: Desc},
}

// Token Methods

// GetString - Returns a string representation of a token
func (tkn Token) GetString() string {
	var tknStr = ""
	switch tkn.tokenID {
	case Create, Table, Select, From, Where, And, Insert, Into, Values, RWTrue,
		RWFalse, Or, Not, Delete, Count, Null, Drop, Update, Set, Order, By, Asc, Desc:
		tknStr = tkn.tokenID
	case TypeTKN:
		tknStr = tkn.tokenValue
	case Asterix, Period, Equal, OpenBracket, CloseBracket, Comma, Colon, UnderScore,
		SemiColon, LessThan, GreaterThan, Minus:
		tknStr = tkn.tokenValue
	case Ident:
		tknStr = "[" + Ident + "=" + tkn.tokenValue + "]"
	default:
		tknStr = "[" + tkn.tokenID + ", " + tkn.tokenValue + "]"

	}
	return tknStr
}

// GetName - Returns a string with the name of the token
func (tkn *Token) GetName() string {
	return tkn.tokenID
}

// GetValue - Returns string with the value of token
func (tkn *Token) GetValue() string {
	return tkn.tokenValue
}

// SetValue - replaces the value of token with v
func (tkn *Token) SetValue(v string) {
	tkn.tokenValue = v
}

//CreateToken - Returns a token based on name. value inputs
func CreateToken(name string, value string) *Token {
	return &Token{tokenID: name, tokenValue: value}
}
func isWhiteSpace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func getWhiteSpace(r []rune) ([]rune, *Token) {
	tkn := CreateToken(Ws, "")

	// loop until no more whitespace
	for {
		//make sure that there is a rune to process
		if len(r) <= 0 || !isWhiteSpace(r[0]) {
			return r, tkn
		}

		tkn.tokenValue = tkn.tokenValue + string(r[0])
		r = r[1:]

	}
}

func checkKeyWords(word string) *Token {
	tkn, ok := Words[strings.ToUpper(word)]
	if !ok {
		tkn = CreateToken(Ident, word)
	}
	return tkn
}

func isLetter(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}
func getIdentifier(r []rune) ([]rune, *Token) {
	word := ""

	// loop until identifier complete
	for {
		//make sure that there is a rune to process
		if len(r) <= 0 || !(isLetter(r[0]) || isDigit(r[0]) || isUnderScore(r[0])) {
			tkn := checkKeyWords(word)
			return r, tkn
		}

		word += string(r[0])
		r = r[1:]

	}
}
func isQuote(ch rune) bool {
	return (ch == '"')
}

func isUnderScore(ch rune) bool {
	return (ch == '_')
}

func getQuote(r []rune) ([]rune, *Token) {
	var quoteVal = ""

	//eat first quote
	r = r[1:]

	// loop until next quote
	for {
		//make sure that there is a rune to process
		if len(r) <= 0 {
			//did not find end of quote
			return r, CreateToken(Err, "Missing End Quote")
		}

		if isQuote(r[0]) {
			//found the end of quote
			r = r[1:]
			//break
			return r, CreateToken(Quote, quoteVal)
		}
		quoteVal = quoteVal + string(r[0])
		r = r[1:]

	}
}
func isDigit(ch rune) bool {
	return (ch >= '0' && ch <= '9')
}
func getNumber(r []rune) ([]rune, *Token) {
	tkn := CreateToken(Num, "")
	hasDecimal := false
	// loop until no more digits
	for {
		char := string(r[0])
		tkn.tokenValue += char
		r = r[1:]
		//make sure that there is a rune to process
		if len(r) <= 0 || !(isDigit(r[0]) || r[0] == '.' && !hasDecimal) {
			return r, tkn
		}

		// Allow only one decimal point
		hasDecimal = hasDecimal || char == "."
	}
}

// Tokenize - Create a token list given a string
func Tokenize(str string) *TokenList {
	var tkn *Token

	log.Debug("Parsing..." + str)

	tl := NewTokenList()
	r := []rune(str)

	for {
		//make sure that there is a rune to process
		if r == nil || len(r) <= 0 {
			break
		}
		//test for whitespace and condense into one token
		if isWhiteSpace(r[0]) {
			r, tkn = getWhiteSpace(r)
			continue
		}
		if isLetter(r[0]) || isUnderScore(r[0]) {
			r, tkn = getIdentifier(r)
			tl.Add(tkn)
			continue
		}

		if isQuote(r[0]) {
			// add QUOTE token
			r, tkn = getQuote(r)
			tl.Add(tkn)
			continue

		}

		if isDigit(r[0]) || r[0] == '-' {
			r, tkn = getNumber(r)
			tl.Add(tkn)
			continue
		}

		//check to see if it is a symbol
		if Symbl, isSymbl := SYMBOLS[r[0]]; isSymbl {
			tl.Add(Symbl)
			r = r[1:]
			continue
		}

		tl.Add(CreateToken(Unk, string(r[0])))
		r = r[1:]

	}

	log.Trace("Finished Parsing...", tl.ToString())
	return tl

}
