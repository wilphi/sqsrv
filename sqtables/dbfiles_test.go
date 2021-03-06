package sqtables_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/sqtest"
)

func init() {
	sqtest.TestInit("sqtables_test.log")
}
func TestNextLargerBlock(t *testing.T) {
	vals := []int{0, 5, 64, 20000, 1000000, 1048599}
	exp := []int64{64, 64, 128, 32768, 1048576, 0}
	var ret int64
	for i, num := range vals {
		ret = sqtables.NextLargerBlock(num)
		log.Printf("Initial: %d, Expected: %d, Actual: %d\n", num, exp[i], ret)
		if exp[i] != ret {
			t.Errorf("Expected %d does not match Actual %d", exp[i], ret)
		}
	}

}

func TestWrite(t *testing.T) {
	var buff bytes.Buffer
	log.Printf("Buffer Len = %d, Buffer Cap = %d\n", buff.Len(), buff.Cap())
	var str = "123456789"
	err := binary.Write(&buff, binary.LittleEndian, int32(len(str)))
	if err != nil {
		log.Println("Error: ", err)
	}
	log.Printf("Buffer Len = %d, Buffer Cap = %d\n", buff.Len(), buff.Cap())
	i, err := io.WriteString(&buff, str)
	log.Printf("str - i= %d, err=%v\n", i, err)
	log.Printf("Buffer Len = %d, Buffer Cap = %d\n", buff.Len(), buff.Cap())

	err = binary.Write(&buff, binary.LittleEndian, int64(896))
	if err != nil {
		log.Println("Error: ", err)
	}
	log.Printf("Buffer Len = %d, Buffer Cap = %d\n", buff.Len(), buff.Cap())
	str2 := "Start:123456789012345\"678\n901234567890123456789012345678901234567890:End"
	i, err = io.WriteString(&buff, str2)
	log.Printf("str2 - i= %d, err=%v\n", i, err)
	log.Printf("Buffer Len = %d, Buffer Cap = %d\n", buff.Len(), buff.Cap())

	var rlen int32
	var rval int64
	var rstr, rstr2 string

	err = binary.Read(&buff, binary.LittleEndian, &rlen)
	strbuff := make([]byte, rlen)
	err = binary.Read(&buff, binary.LittleEndian, &strbuff)
	rstr = string(strbuff)
	err = binary.Read(&buff, binary.LittleEndian, &rval)
	strbuff2 := make([]byte, len(str2))
	err = binary.Read(&buff, binary.LittleEndian, &strbuff2)
	rstr2 = string(strbuff2)

	log.Printf("Len = %d, Str = %q, val = %d Second STr %q\n", rlen, rstr, rval, rstr2)
	log.Printf("Buffer Len = %d, Buffer Cap = %d\n", buff.Len(), buff.Cap())

}
