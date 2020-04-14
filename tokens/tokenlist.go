package tokens

// TokenList - Structure to contain a list of Tokens
type TokenList struct {
	tkns []Token
}

// Add - Add a token to the list
func (tl *TokenList) Add(tkn Token) {
	tl.tkns = append(tl.tkns, tkn)
}

// Remove - Remove a token from the list
func (tl *TokenList) Remove() {
	if len(tl.tkns) > 0 {
		tl.tkns[0] = nil
		tl.tkns = tl.tkns[1:]
	}
}

// Peek - returns the token at head of list
func (tl *TokenList) Peek() Token {
	if len(tl.tkns) == 0 {
		return nil
	}
	return tl.tkns[0]
}

// Peekx - returns the token at list[x]
func (tl *TokenList) Peekx(x int) Token {
	if len(tl.tkns) <= x || x < 0 {
		return nil
	}
	return tl.tkns[x]
}

// Len - Number of tokens in list
func (tl *TokenList) Len() int {
	return len(tl.tkns)
}

// IsEmpty tests if the token list is empty or not
func (tl *TokenList) IsEmpty() bool {
	return tl.Len() <= 0
}

// String - returns a string representation of list
func (tl *TokenList) String() string {
	output := ""
	for _, tkn := range tl.tkns {
		output = output + tkn.String() + " "
	}

	// Remove trailing space
	if len(output) > 0 {
		output = output[:len(output)-1]
	}

	return output
}

/*
// Test - Test a token to see if it matches one of the tknNames.
//  Returns the value of token if matched otherwise blank
//  If there are no more tokens in list blank is returned as well
func (tl *TokenList) Test(tknNames ...string) string {
	if len(tl.tkns) > 0 {
		for _, tknName := range tknNames {
			if tl.tkns[0].Name() == tknName {
				return tl.tkns[0].GetValue()
			}
		}
	}
	return ""
}
*/

// Test - Test a token to see if it matches one of the tknNames.
//  Returns the token if matched otherwise nil
//  If there are no more tokens in list nil is returned as well
func (tl *TokenList) Test(tkns ...TokenID) Token {
	if len(tl.tkns) > 0 {
		for _, tkn := range tkns {
			if tl.tkns[0].ID() == tkn {
				return tl.tkns[0]
			}
		}
	}
	return nil
}

// IsReservedWord - checks to see if the first token in list is a reserved word token
func (tl *TokenList) IsReservedWord() bool {
	if len(tl.tkns) > 0 {
		return tl.tkns[0].TestFlags(IsWord)
	}
	return false
}

// NewTokenList - Create a new token list
func NewTokenList() *TokenList {
	tl := TokenList{}
	tl.tkns = make([]Token, 0, 100)
	return &tl
}

// CreateList - Creates a new token list from an array of tokens
func CreateList(tkns []Token) *TokenList {
	tl := TokenList{}
	tl.tkns = tkns
	return &tl
}
