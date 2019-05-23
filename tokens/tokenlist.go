package tokens

// TokenList - Structure to contain a list of Tokens
type TokenList struct {
	tkns []*Token
}

// Add - Add a token to the list
func (tl *TokenList) Add(tkn *Token) {
	tl.tkns = append(tl.tkns, tkn)
}

// Remove - Remove a token from the list
func (tl *TokenList) Remove() {
	if len(tl.tkns) > 0 {
		tl.tkns = tl.tkns[1:]
	}
}

// Peek - returns the token at head of list
func (tl *TokenList) Peek() *Token {
	if len(tl.tkns) == 0 {
		return nil
	}
	return tl.tkns[0]
}

// Peekx - returns the token at list[x]
func (tl *TokenList) Peekx(x int) *Token {
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

// ToString - returns a string representation of list
func (tl *TokenList) ToString() string {
	output := ""
	for _, tkn := range tl.tkns {
		output = output + tkn.GetString() + " "
	}

	// Remove trailing space
	if len(output) > 0 {
		output = output[:len(output)-1]
	}

	return output
}

// Test - Test a token to see if it matches one of the tknNames.
//  Returns the value of token if matched otherwise blank
//  If there are no more tokens in list blank is returned as well
func (tl *TokenList) Test(tknNames ...string) string {
	if len(tl.tkns) > 0 {
		for _, tknName := range tknNames {
			if tl.tkns[0].GetName() == tknName {
				return tl.tkns[0].GetValue()
			}
		}
	}
	return ""
}

// NewTokenList - Create a new token list
func NewTokenList() *TokenList {
	tl := TokenList{}
	tl.tkns = make([]*Token, 0, 100)
	return &tl
}
