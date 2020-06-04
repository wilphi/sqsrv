package moniker_test

import (
	"strings"
	"testing"

	"github.com/wilphi/sqsrv/sqtables/moniker"
	"github.com/wilphi/sqsrv/sqtest"
)

func TestMoniker(t *testing.T) {
	defer sqtest.PanicTestRecovery(t, "")

	name1 := "TableA"
	alias1 := "AliasA"
	m1 := moniker.New(name1, alias1)
	m2 := m1.Clone()
	m3 := moniker.New(name1, "differentAlias")
	t.Run("lower case test", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if strings.ToLower(name1) != m1.Name() {
			t.Errorf("Name should be lower case: %s", m1.Name())
		}
		if strings.ToLower(alias1) != m1.Alias() {
			t.Errorf("Alias should be lower case: %s", m1.Alias())
		}

	})

	t.Run("Equal Tests", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		if moniker.Equal(nil, nil) {
			t.Errorf("moniker.Equal was not false")
		}
		if moniker.Equal(m1, nil) {
			t.Errorf("moniker.Equal was not false")
		}
		if moniker.Equal(nil, m1) {
			t.Errorf("moniker.Equal was not false")
		}
		if !moniker.Equal(m1, m2) {
			t.Error("m1 & m2 should be identical")
		}
		if moniker.Equal(m1, m3) {
			t.Error("m1 & m3 have different alias")
		}
	})

	t.Run("Display tests", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		name := "tab"
		noAlias := moniker.New(name, "")

		// both String and Show should be equal to the name
		if noAlias.String() != name {
			t.Errorf("String (%s) is not equal to name: %s", noAlias.String(), name)

		}
		if noAlias.Show() != name {
			t.Errorf("Show (%s) is not equal to name: %s", noAlias.String(), name)

		}

		// Now set the alias and check
		alias := "test"
		noAlias.SetAlias(alias)
		strRes := alias + "<" + name + ">"
		if noAlias.String() != strRes {
			t.Errorf("String with alias (%s) is not equal to: %s", noAlias.String(), strRes)

		}
		if noAlias.Show() != alias {
			t.Errorf("Show with alias (%s) is not equal to: %s", noAlias.String(), alias)

		}

	})
}
