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
	Having
	Inner
	Join
	On
	Full
	Outer
	Left
	Right
	Cross
	Primary
	Key
	Unique
	Foreign
	Index
	Begin
	Commit
	Rollback
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
	"!=", "<=", ">=", "HAVING", "INNER", "JOIN", "ON",
	"FULL", "OUTER", "LEFT", "RIGHT", "CROSS",
	"PRIMARY", "KEY", "UNIQUE", "FOREIGN", "INDEX",
	"BEGIN", "COMMIT", "ROLLBACK",
}

//wordTokens -
var wordTokens map[TokenID]Token

//WordMap will map a string to a token
var WordMap map[string]Token

// AllTypes is an array of all Type tokens
var AllTypes []TokenID

func init() {

	wordTokens = map[TokenID]Token{
		Create:           newWordToken(Create, IsWord),
		Table:            newWordToken(Table, IsWord),
		Select:           newWordToken(Select, IsWord),
		From:             newWordToken(From, IsWord),
		Where:            newWordToken(Where, IsWord),
		And:              newWordToken(And, IsWord),
		Insert:           newWordToken(Insert, IsWord),
		Into:             newWordToken(Into, IsWord),
		Values:           newWordToken(Values, IsWord),
		RWTrue:           newWordToken(RWTrue, IsWord),
		RWFalse:          newWordToken(RWFalse, IsWord),
		Not:              newWordToken(Not, IsWord),
		Or:               newWordToken(Or, IsWord),
		Delete:           newWordToken(Delete, IsWord),
		Count:            newWordToken(Count, IsWord|IsFunction|IsOneArg|IsNoArg|IsAggregate),
		Null:             newWordToken(Null, IsWord),
		Drop:             newWordToken(Drop, IsWord),
		Update:           newWordToken(Update, IsWord),
		Set:              newWordToken(Set, IsWord),
		Order:            newWordToken(Order, IsWord),
		By:               newWordToken(By, IsWord),
		Asc:              newWordToken(Asc, IsWord),
		Desc:             newWordToken(Desc, IsWord),
		Distinct:         newWordToken(Distinct, IsWord),
		Group:            newWordToken(Group, IsWord),
		Min:              newWordToken(Min, IsWord|IsFunction|IsOneArg|IsAggregate),
		Max:              newWordToken(Max, IsWord|IsFunction|IsOneArg|IsAggregate),
		Avg:              newWordToken(Avg, IsWord|IsFunction|IsOneArg|IsAggregate),
		Sum:              newWordToken(Sum, IsWord|IsFunction|IsOneArg|IsAggregate),
		Int:              newWordToken(Int, IsWord|IsType|IsFunction|IsOneArg),
		String:           newWordToken(String, IsWord|IsType|IsFunction|IsOneArg),
		Bool:             newWordToken(Bool, IsWord|IsType|IsFunction|IsOneArg),
		Float:            newWordToken(Float, IsWord|IsType|IsFunction|IsOneArg),
		Asterix:          newWordToken(Asterix, IsSymbol),
		Period:           newWordToken(Period, IsSymbol),
		Equal:            newWordToken(Equal, IsSymbol),
		OpenBracket:      newWordToken(OpenBracket, IsSymbol),
		CloseBracket:     newWordToken(CloseBracket, IsSymbol),
		Comma:            newWordToken(Comma, IsSymbol),
		Colon:            newWordToken(Colon, IsSymbol),
		UnderScore:       newWordToken(UnderScore, IsSymbol),
		SemiColon:        newWordToken(SemiColon, IsSymbol),
		LessThan:         newWordToken(LessThan, IsSymbol),
		GreaterThan:      newWordToken(GreaterThan, IsSymbol),
		Minus:            newWordToken(Minus, IsSymbol),
		Plus:             newWordToken(Plus, IsSymbol),
		Divide:           newWordToken(Divide, IsSymbol),
		Modulus:          newWordToken(Modulus, IsSymbol),
		NotEqual:         newWordToken(NotEqual, IsSymbol),
		LessThanEqual:    newWordToken(LessThanEqual, IsSymbol),
		GreaterThanEqual: newWordToken(GreaterThanEqual, IsSymbol),
		Having:           newWordToken(Having, IsWord),
		Inner:            newWordToken(Inner, IsWord),
		Join:             newWordToken(Join, IsWord),
		On:               newWordToken(On, IsWord),
		Full:             newWordToken(Full, IsWord),
		Outer:            newWordToken(Outer, IsWord),
		Left:             newWordToken(Left, IsWord),
		Right:            newWordToken(Right, IsWord),
		Cross:            newWordToken(Cross, IsWord),
		Primary:          newWordToken(Primary, IsWord),
		Key:              newWordToken(Key, IsWord),
		Unique:           newWordToken(Unique, IsWord),
		Foreign:          newWordToken(Foreign, IsWord),
		Index:            newWordToken(Index, IsWord),
		Begin:            newWordToken(Begin, IsWord),
		Commit:           newWordToken(Commit, IsWord),
		Rollback:         newWordToken(Rollback, IsWord),
	}
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
	tName   string
	flags   TokenFlags
}

func newWordToken(tokenID TokenID, flags TokenFlags) *WordToken {
	return &WordToken{tokenID: tokenID, tName: IDName(tokenID), flags: flags}
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
