package moniker

import "strings"

// Moniker is the name/alias pair for an object such as a table
type Moniker struct {
	name  string
	alias string
}

// New creates a new Moniker
func New(name, alias string) *Moniker {
	return &Moniker{name: strings.ToLower(name), alias: strings.ToLower(alias)}
}

// Equal compares two Moniker pairs for equality
func Equal(a, b *Moniker) bool {
	if a == nil || b == nil {
		return false
	}
	return a.name == b.name && a.alias == b.alias
}

// Name returns the name of the moniker
func (na Moniker) Name() string {
	return na.name
}

// Alias returns the alias of the moniker
func (na Moniker) Alias() string {
	return na.alias
}

// SetAlias changes the alias to the given string
func (na *Moniker) SetAlias(a string) {
	na.alias = strings.ToLower(a)
}

// Show returns the alias if it exists otherwise the name
func (na Moniker) Show() string {
	if na.alias != "" {
		return na.alias
	}
	return na.name
}

// String returns a string representation of the moniker
func (na Moniker) String() string {
	if na.alias == "" {
		return na.name
	}
	return na.alias + "<" + na.name + ">"
}

// Clone returns a copy of the moniker
func (na Moniker) Clone() *Moniker {
	return New(na.name, na.alias)
}
