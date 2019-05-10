package transid

import "testing"

func TestTransID(t *testing.T) {

	t.Run("Get transid", func(t *testing.T) {
		if GetTransID() != 0 {
			t.Error("TransID should be 0")
		}
	})
	t.Run("Get next transid", func(t *testing.T) {
		if GetNextID() != 1 {
			t.Error("TransID should be 1")
		}
		if GetTransID() != 1 {
			t.Error("TransID should be 1")
		}
	})
	t.Run("Get next transid again", func(t *testing.T) {
		if GetNextID() != 2 {
			t.Error("TransID should be 2")
		}
		if GetTransID() != 2 {
			t.Error("TransID should be 2")
		}
	})
	t.Run("Set transid", func(t *testing.T) {
		SetTransID(1234)
		if GetTransID() != 1234 {
			t.Error("TransID should be 1234")
		}
	})

	t.Run("Get after Set", func(t *testing.T) {
		var val uint64 = 998

		SetTransID(val)
		if GetTransID() != val {
			t.Errorf("TransID should be %d", val)
		}
		if GetNextID() != val+1 {
			t.Errorf("TransID should be %d", val+1)
		}
		if GetTransID() != val+1 {
			t.Errorf("TransID should be %d", val+1)
		}
	})

}
