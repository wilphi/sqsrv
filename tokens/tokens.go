package tokens

import (
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

// TokenID type
type TokenID uint8

// TokenFlags type
type TokenFlags uint8

// Token token interface
type Token interface {
	ID() TokenID
	Name() string
	String() string
	TestFlags(mask TokenFlags) bool
}

// Token Flags
const (
	IsWord = 1 << iota
	IsSymbol
	IsType
	IsFunction
	IsAggregate
	IsNoArg
	IsOneArg
)

//Type Lengths
const (
	LenInt   = 10
	LenStr   = 20
	LenBool  = 5
	LenFloat = 20
)

// allTokenNames - list of token names by ID
var allTokenNames map[TokenID]string

func init() {
	allTokenNames = make(map[TokenID]string)
	for i, name := range wordNames {
		allTokenNames[TokenID(i)] = name
	}
	for i, name := range valueTokenNames {
		allTokenNames[i] = name
	}
}

//IDName gets a string representation of the ID
func IDName(ID TokenID) string {
	s, ok := allTokenNames[ID]
	if !ok {
		return fmt.Sprintf("ID-%d (not found)", ID)
	}
	return s
}

func isWhiteSpace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func getWhiteSpace(r []rune) []rune {
	// loop until no more whitespace
	for {
		//make sure that there is a rune to process
		if len(r) <= 0 || !isWhiteSpace(r[0]) {
			break
		}
		r = r[1:]

	}
	return r
}

func checkKeyWords(word string) Token {
	tkn, ok := WordMap[strings.ToUpper(word)]
	if !ok {
		tkn = NewValueToken(Ident, word)
	}
	return tkn
}

func isLetter(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}
func getIdentifier(r []rune) ([]rune, Token) {
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

func getQuote(r []rune) ([]rune, Token) {
	var quoteVal = ""

	//eat first quote
	r = r[1:]

	// loop until next quote
	for {
		//make sure that there is a rune to process
		if len(r) <= 0 {
			//did not find end of quote
			return r, NewValueToken(Err, "Missing End Quote")
		}

		if isQuote(r[0]) {
			//found the end of quote
			r = r[1:]
			//break
			return r, NewValueToken(Quote, quoteVal)
		}
		quoteVal = quoteVal + string(r[0])
		r = r[1:]

	}
}
func isDigit(ch rune) bool {
	return (ch >= '0' && ch <= '9')
}
func getNumber(r []rune) ([]rune, Token) {
	num := ""

	hasDecimal := false
	// loop until no more digits
	for {
		char := string(r[0])
		num += char
		r = r[1:]
		//make sure that there is a rune to process
		if len(r) <= 0 || !(isDigit(r[0]) || r[0] == '.' && !hasDecimal) {
			return r, NewValueToken(Num, num)
		}

		// Allow only one decimal point
		hasDecimal = hasDecimal || char == "."
	}
}

// Tokenize - Create a token list given a string
func Tokenize(str string) *TokenList {
	var tkn Token

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
			r = getWhiteSpace(r)
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

		if isDigit(r[0]) {
			r, tkn = getNumber(r)
			tl.Add(tkn)
			continue
		}

		//check to see if it is a symbol
		// Check double char symbols first
		if len(r) > 1 {
			if Symbl, isSymbl := WordMap[string(r[0])+string(r[1])]; isSymbl {
				tl.Add(Symbl)
				r = r[2:]
				continue
			}
		}
		// now single char symbols
		if Symbl, isSymbl := WordMap[string(r[0])]; isSymbl {
			tl.Add(Symbl)
			r = r[1:]
			continue
		}

		tl.Add(NewValueToken(Unk, string(r[0])))
		r = r[1:]

	}

	log.Trace("Finished Parsing...", tl.String())
	return tl

}
