package moniker

import "strings"

// Moniker is the name/alias pair for an object such as a table
type Moniker struct {
	Name  string
	Alias string
}

// New creates a new Moniker
func New(name, alias string) *Moniker {
	return &Moniker{Name: strings.ToLower(name), Alias: strings.ToLower(alias)}
}

// Equal compares two Moniker pairs for equality
func Equal(a, b *Moniker) bool {
	if a == nil || b == nil {
		return false
	}
	return strings.ToLower(a.Name) == strings.ToLower(b.Name) && strings.ToLower(a.Alias) == strings.ToLower(b.Alias)
}

// SameName compares two Moniker pairs to see if they have the same name but not alias
func SameName(a, b *Moniker) bool {
	if a == nil || b == nil {
		return false
	}

	return a.Name == b.Name && a.Alias != b.Alias
}

// Show returns the alias if it exists otherwise the name
func (na Moniker) Show() string {
	if na.Alias != "" {
		return na.Alias
	}
	return na.Name
}

// String returns a string representation of the moniker
func (na Moniker) String() string {
	if na.Alias == "" {
		return na.Name
	}
	return na.Alias + "<" + na.Name + ">"
}

// Clone returns a copy of the moniker
func (na Moniker) Clone() *Moniker {
	return New(na.Name, na.Alias)
}
