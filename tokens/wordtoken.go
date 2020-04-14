package tokens

import (
	"strings"

	log "github.com/sirupsen/logrus"
)

// Reserved Words
const (
	NilToken = iota
	Create
	Table
	Select
	From
	Where
	And
	Insert
	Into
	Values
	RWTrue
	RWFalse
	Not
	Or
	Delete
	Count
	Null
	Drop
	Update
	Set
	Order
	By
	Asc
	Desc
	Distinct
	Group
	Min
	Max
	Avg
	Sum
	Int
	String
	Bool
	Float
	Asterix
	Period
	Equal
	OpenBracket
	CloseBracket
	Comma
	Colon
	UnderScore
	SemiColon
	LessThan
	GreaterThan
	Minus
	Plus
	Divide
	Modulus
	NotEqual
	LessThanEqual
	GreaterThanEqual
)

var wordNames = []string{"Invalid", "CREATE", "TABLE",
	"SELECT", "FROM", "WHERE", "AND",
	"INSERT", "INTO", "VALUES", "TRUE",
	"FALSE", "NOT", "OR", "DELETE", "COUNT",
	"NULL", "DROP", "UPDATE", "SET", "ORDER",
	"BY", "ASC", "DESC", "DISTINCT", "GROUP",
	"MIN", "MAX", "AVG", "SUM", "INT",
	"STRING", "BOOL", "FLOAT",
	"*", ".", "=", "(", ")", ",", ":", "_", ";",
	"<", ">", "-", "+", "/", "%",
	"!=", "<=", ">=",
}

//wordTokens -
var wordTokens = map[TokenID]Token{
	Create:           &WordToken{tokenID: Create, flags: IsWord},
	Table:            &WordToken{tokenID: Table, flags: IsWord},
	Select:           &WordToken{tokenID: Select, flags: IsWord},
	From:             &WordToken{tokenID: From, flags: IsWord},
	Where:            &WordToken{tokenID: Where, flags: IsWord},
	And:              &WordToken{tokenID: And, flags: IsWord},
	Insert:           &WordToken{tokenID: Insert, flags: IsWord},
	Into:             &WordToken{tokenID: Into, flags: IsWord},
	Values:           &WordToken{tokenID: Values, flags: IsWord},
	RWTrue:           &WordToken{tokenID: RWTrue, flags: IsWord},
	RWFalse:          &WordToken{tokenID: RWFalse, flags: IsWord},
	Not:              &WordToken{tokenID: Not, flags: IsWord},
	Or:               &WordToken{tokenID: Or, flags: IsWord},
	Delete:           &WordToken{tokenID: Delete, flags: IsWord},
	Count:            &WordToken{tokenID: Count, flags: IsWord | IsFunction | IsOneArg | IsNoArg | IsAggregate},
	Null:             &WordToken{tokenID: Null, flags: IsWord},
	Drop:             &WordToken{tokenID: Drop, flags: IsWord},
	Update:           &WordToken{tokenID: Update, flags: IsWord},
	Set:              &WordToken{tokenID: Set, flags: IsWord},
	Order:            &WordToken{tokenID: Order, flags: IsWord},
	By:               &WordToken{tokenID: By, flags: IsWord},
	Asc:              &WordToken{tokenID: Asc, flags: IsWord},
	Desc:             &WordToken{tokenID: Desc, flags: IsWord},
	Distinct:         &WordToken{tokenID: Distinct, flags: IsWord},
	Group:            &WordToken{tokenID: Group, flags: IsWord},
	Min:              &WordToken{tokenID: Min, flags: IsWord | IsFunction | IsOneArg | IsAggregate},
	Max:              &WordToken{tokenID: Max, flags: IsWord | IsFunction | IsOneArg | IsAggregate},
	Avg:              &WordToken{tokenID: Avg, flags: IsWord | IsFunction | IsOneArg | IsAggregate},
	Sum:              &WordToken{tokenID: Sum, flags: IsWord | IsFunction | IsOneArg | IsAggregate},
	Int:              &WordToken{tokenID: Int, flags: IsWord | IsType | IsFunction | IsOneArg},
	String:           &WordToken{tokenID: String, flags: IsWord | IsType | IsFunction | IsOneArg},
	Bool:             &WordToken{tokenID: Bool, flags: IsWord | IsType | IsFunction | IsOneArg},
	Float:            &WordToken{tokenID: Float, flags: IsWord | IsType | IsFunction | IsOneArg},
	Asterix:          &WordToken{tokenID: Asterix, flags: IsSymbol},
	Period:           &WordToken{tokenID: Period, flags: IsSymbol},
	Equal:            &WordToken{tokenID: Equal, flags: IsSymbol},
	OpenBracket:      &WordToken{tokenID: OpenBracket, flags: IsSymbol},
	CloseBracket:     &WordToken{tokenID: CloseBracket, flags: IsSymbol},
	Comma:            &WordToken{tokenID: Comma, flags: IsSymbol},
	Colon:            &WordToken{tokenID: Colon, flags: IsSymbol},
	UnderScore:       &WordToken{tokenID: UnderScore, flags: IsSymbol},
	SemiColon:        &WordToken{tokenID: SemiColon, flags: IsSymbol},
	LessThan:         &WordToken{tokenID: LessThan, flags: IsSymbol},
	GreaterThan:      &WordToken{tokenID: GreaterThan, flags: IsSymbol},
	Minus:            &WordToken{tokenID: Minus, flags: IsSymbol},
	Plus:             &WordToken{tokenID: Plus, flags: IsSymbol},
	Divide:           &WordToken{tokenID: Divide, flags: IsSymbol},
	Modulus:          &WordToken{tokenID: Modulus, flags: IsSymbol},
	NotEqual:         &WordToken{tokenID: NotEqual, flags: IsSymbol},
	LessThanEqual:    &WordToken{tokenID: LessThanEqual, flags: IsSymbol},
	GreaterThanEqual: &WordToken{tokenID: GreaterThanEqual, flags: IsSymbol},
}

//WordMap will map a string to a token
var WordMap map[string]Token

// AllTypes is an array of all Type tokens
var AllTypes []TokenID

func init() {
	// create the word map of reserved words and symbols
	// making sure that all words are uppercase
	WordMap = make(map[string]Token)
	for i, word := range wordTokens {
		if i != 0 {
			WordMap[strings.ToUpper(wordNames[i])] = word

			// Add any type tokens to AllTypes
			if word.TestFlags(IsType) {
				AllTypes = append(AllTypes, word.ID())
			}
		}
	}

}

// WordToken is for reserved words
type WordToken struct {
	tokenID TokenID
	flags   TokenFlags
}

// ID returns the Id of the token
func (tkn *WordToken) ID() TokenID {
	return tkn.tokenID
}

// Name returns the text name of the token
func (tkn *WordToken) Name() string {
	return wordNames[tkn.tokenID]
}

// String returns a string representation of the token
//   This may or may not be the same as the token Name
func (tkn *WordToken) String() string {
	return wordNames[tkn.tokenID]
}

// TestFlags -
func (tkn *WordToken) TestFlags(mask TokenFlags) bool {
	return (tkn.flags & mask) == mask
}

// GetWordToken returns the token based on ID
func GetWordToken(ID TokenID) Token {
	tkn, ok := wordTokens[ID]
	if !ok {
		log.Panicf("ID: %s is not a valid WorkToken id", IDName(ID))

	}
	return tkn
}
