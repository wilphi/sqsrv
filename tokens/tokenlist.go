package tokens

// TokenList - Structure to contain a list of Tokens
type TokenList struct {
	tkns []*Token
}

// CreateTokenListFromTokens -
func CreateTokenListFromTokens(tkns []*Token) *TokenList {
	tl := TokenList{}
	tl.tkns = tkns
	return &tl
}

// Add - Add a token to the list
func (tl *TokenList) Add(tkn *Token) {
	tl.tkns = append(tl.tkns, tkn)
}

// Remove - Remove a token from the list
func (tl *TokenList) Remove() {
	//<<<Watch out for Empty List>>>
	tl.tkns = tl.tkns[1:]
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

// ToString - returns a string representation of list
func (tl *TokenList) ToString() string {
	output := ""
	for _, tkn := range tl.tkns {
		output = output + tkn.GetString() + " "
	}
	if len(output) > 0 {
		output = output[:len(output)-1]
	} else {
		output = ""
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

// Insert - Insert given tokens at idx
func (tl *TokenList) Insert(idx int, newTkns ...*Token) {
	t := make([]*Token, len(tl.tkns)+len(newTkns))
	offset := 0
	for i, tkn := range tl.tkns {
		t[i+offset] = tkn
		if i+offset == idx {
			for _, ntkn := range newTkns {
				offset++
				t[i+offset] = ntkn
			}
		}
	}
	tl.tkns = t
	return
}

// Delete - Delete num tokens starting at idx
func (tl *TokenList) Delete(idx, num int) {
	t := make([]*Token, len(tl.tkns)-num)
	i := 0
	for j, tkn := range tl.tkns {
		if j < idx || j > idx+num-1 {
			t[i] = tkn
			i++
		}
	}
	tl.tkns = t
}

// NewTokenList - Create a new token list
func NewTokenList() *TokenList {
	tl := TokenList{}
	tl.tkns = make([]*Token, 0, 100)
	return &tl
}
