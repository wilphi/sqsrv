package redo

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/transid"

	"github.com/wilphi/sqsrv/sqtables"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/sqerr"
)

const (
	IDTestStmt    = 250
	IDTestWithErr = 251
)

var Items []string

// TestStmt -
type TestStmt struct {
	Str string
	ID  uint64
}

func init() {
	DecodeStatementHook = func(b byte) LogStatement {
		var stmt LogStatement
		switch b {
		case IDTestStmt:
			stmt = &TestStmt{}
		case IDTestWithErr:
			stmt = &TestWithErr{}
		default:
			panic(fmt.Sprintf("Unknown LogStatement Type %d", b))
		}
		return stmt
	}
}

// Encode uses sqbin.Codec to return a binary encoded version of the statement
func (t *TestStmt) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	// Identify the type of logstatment
	enc.Writebyte(IDTestStmt)
	// Id of transaction statement
	enc.WriteUint64(t.ID)

	enc.WriteString(t.Str)
	return enc
}

// Decode uses sqbin.Codec to return a binary encoded version of the statement
func (t *TestStmt) Decode(dec *sqbin.Codec) {

	mkr := dec.Readbyte()
	if mkr != IDTestStmt {
		log.Panicf("Found wrong statement type (%s). Expecting IDTestStmt", sqbin.TypeMarkerStrings[mkr])
	}
	// Id of transaction statement
	t.ID = dec.ReadUint64()

	t.Str = dec.ReadString()

}

// Recreate for TestStmt
func (t *TestStmt) Recreate(profile *sqprofile.SQProfile) error {
	Items = append(Items, "Recreated "+t.Str)
	return nil
}

// Identify for TestStmt
func (t *TestStmt) Identify() string {
	return "Test Statement: " + t.Str
}

// SetID is used by the transaction logger to indicate the what order the message was sent to the
//   transaction log. It should be a monotonically increasing number.
func (t *TestStmt) SetID(id uint64) {
	t.ID = id
}

// GetID returns the ID of the transaction statement. ID = 0 means that it is not valid
func (t *TestStmt) GetID() uint64 {
	return t.ID
}

// TestWithErr - for testing error paths in software
type TestWithErr struct {
	Str string
	ID  uint64
}

// Encode uses sqbin.Codec to return a binary encoded version of the statement
func (t *TestWithErr) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	// Identify the type of logstatment
	enc.Writebyte(IDTestWithErr)
	// Id of transaction statement
	enc.WriteUint64(t.ID)

	enc.WriteString(t.Str)
	return enc
}

// Decode uses sqbin.Codec to return a binary encoded version of the statement
func (t *TestWithErr) Decode(dec *sqbin.Codec) {
	mkr := dec.Readbyte()
	if mkr != IDTestWithErr {
		log.Panicf("Found wrong statement type (%s). Expecting IDTestWithErr", sqbin.TypeMarkerStrings[mkr])
	}

	// Id of transaction statement
	t.ID = dec.ReadUint64()

	t.Str = dec.ReadString()

}

// Recreate TestWithErr for testing error paths
func (t *TestWithErr) Recreate(profile *sqprofile.SQProfile) error {
	Items = append(Items, "Not Recreated "+t.Str)

	return sqerr.NewInternal("Testing Error Handling: " + t.Str)
}

// Recreate TestWithErr for testing error paths
func (t *TestWithErr) Identify() string {
	return "Test With Error: " + t.Str
}

// SetID is used by the transaction logger to indicate the what order the message was sent to the
//   transaction log. It should be a monotonically increasing number.
func (t *TestWithErr) SetID(id uint64) {
	t.ID = id
}

// GetID returns the ID of the transaction statement. ID = 0 means that it is not valid
func (t *TestWithErr) GetID() uint64 {
	return t.ID
}

// TestMain - Setup logging for tests to make sure it does not go to stdio
func TestMain(m *testing.M) {
	// setup logging
	logFile, err := os.OpenFile("redo_test.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(logFile)

	os.Exit(m.Run())

}

type InterfaceData struct {
	TestName string
	i        interface{}
}

func TestInterfaces(t *testing.T) {

	data := []InterfaceData{
		{"Create is a LogStatement", &CreateDDL{}},
		{"Insert is a LogStatement", &InsertRows{}},
		{"Update is a LogStatement", &UpdateRows{}},
		{"Delete is a LogStatement", &DeleteRows{}},
		{"TestStmt is a LogStatement", &TestStmt{}},
		{"TestWithErr is a LogStatement", &TestWithErr{}},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testInterfacesFunc(row))

	}
}

func testInterfacesFunc(d InterfaceData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		_, ok := d.i.(LogStatement)
		if !ok {
			t.Error("Object is not a LogStatement")
		}

	}
}

func TestReadTlog(t *testing.T) {
	profile := sqprofile.CreateSQProfile()

	var data = []TestData{
		{
			TestName: "Recreate",
			Stmts:    []LogStatement{&TestStmt{"Test0", 1}, &TestStmt{"Test1", 2}, &TestStmt{"Test2", 3}, &TestStmt{"Test3", 4}, &TestStmt{"Test4", 5}},
			ErrAfter: -1,
			ExpErr:   "",
			ExpItems: []string{"Recreated Test0", "Recreated Test1", "Recreated Test2", "Recreated Test3", "Recreated Test4"},
		},
		{
			TestName: "Error Recreating",
			Stmts:    []LogStatement{&TestStmt{"ErrTest0", 1}, &TestWithErr{"ErrTest1", 2}},
			ErrAfter: -1,
			ExpErr:   "Internal Error: Testing Error Handling: ErrTest1",
			ExpItems: []string{"Recreated ErrTest0", "Not Recreated ErrTest1"},
		},
		{
			TestName: "Error Reading Log",
			Stmts:    []LogStatement{&TestStmt{"Test0", 1}, &TestStmt{"Test1", 2}, &TestStmt{"Test2", 3}},
			ErrAfter: 2,
			ExpPanic: true,
			ExpErr:   "",
			ExpItems: []string{"Recreated Test0", "Recreated Test1"},
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testReadTlogFunc(profile, row))

	}
}

type TestData struct {
	TestName string
	Stmts    []LogStatement
	ExpItems []string
	ExpErr   string
	ErrAfter int
	ExpPanic bool
}

func testReadTlogFunc(profile *sqprofile.SQProfile, d TestData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if d.ExpPanic && r == nil {
				t.Error(d.TestName + " did not panic")
			}
			if !d.ExpPanic && r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		var err error
		transid.SetTransID(0)
		Items = []string{}
		enc := sqbin.NewCodec(nil)
		for i, stmt := range d.Stmts {
			if i == d.ErrAfter {
				enc.WriteString("Test Object")
			} else {
				tmpenc := stmt.Encode()
				enc.WriteInt64(int64(tmpenc.Len()))
				enc.Write(tmpenc.Bytes())
			}
			if err != nil {
				t.Fatal("Unexpected Error while encoding during test: ", err)
			}
		}
		b := enc.Bytes()
		file := bytes.NewBuffer(b)
		err = ReadTlog(profile, file)
		if err != nil {
			log.Println(err.Error())
			if d.ExpErr == "" {
				t.Error(fmt.Sprintf("Unexpected Error in test: %s", err.Error()))
				return
			}
			if d.ExpErr != err.Error() {
				t.Error(fmt.Sprintf("Expecting Error %s but got: %s", d.ExpErr, err.Error()))
				return
			}
			//return
		}
		if err == nil && d.ExpErr != "" {
			t.Error(fmt.Sprintf("Unexpected Success, should have returned error: %s", d.ExpErr))
			return
		}

		// Verify Recreated Items
		for i := range d.ExpItems {
			if d.ExpItems[i] != Items[i] {
				t.Error(fmt.Sprintf("Recreated log statements do not match: Expected %q but got %q at index: %d", d.ExpItems[i], Items[i], i))
				return
			}
		}
	}
}

func TestRecovery(t *testing.T) {
	var data = []RecoveryData{
		/*	{
			TestName:        "Logging started",
			TransLogName:    "transaction.tlog",
			RecoveryLogName: "recovery.tlog",
			Started:         true,
			ExpPanic:        true,
			ExpErr:          "",
		},*/
		{
			TestName:        "No Logs",
			TransLogName:    "transaction.tlog",
			RecoveryLogName: "recovery.tlog",
			Started:         false,
			ExpPanic:        false,
			ExpErr:          "",
			Profile:         sqprofile.CreateSQProfile(),
		},
		/*		{
					TestName:        "Both Logs",
					TransLogName:    "transaction.tlog",
					RecoveryLogName: "recovery.tlog",
					Started:         false,
					SourceFile:      "./test_files/transaction.tlog",
					CopyToTrans:     true,
					CopyToRecovery:  true,
					ExpPanic:        false,
					ExpErr:          "Error: Both the transaction log and recovery log exist",
					Profile:         sqprofile.CreateSQProfile(),
				},
				{
					TestName:        "Recovery only Log",
					TransLogName:    "transaction.tlog",
					RecoveryLogName: "recovery.tlog",
					Started:         false,
					SourceFile:      "./test_files/transaction.tlog",
					CopyToTrans:     false,
					CopyToRecovery:  true,
					ExpPanic:        false,
					ExpErr:          "",
					Profile:         sqprofile.CreateSQProfile(),
				},*/
		/*		{
				TestName:        "Trans only Log",
				TransLogName:    "transaction.tlog",
				RecoveryLogName: "recovery.tlog",
				Started:         false,
				SourceFile:      "./test_files/transaction.tlog",
				CopyToTrans:     true,
				CopyToRecovery:  false,
				ExpPanic:        false,
				ExpErr:          "",
				Profile:         sqprofile.CreateSQProfile(),
			},*/
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testRecoveryFunc(row))

	}

}

type RecoveryData struct {
	TestName        string
	TransLogName    string
	RecoveryLogName string
	SourceFile      string
	CopyToTrans     bool
	CopyToRecovery  bool
	Started         bool
	ExpPanic        bool
	ExpErr          string
	Profile         *sqprofile.SQProfile
}

func copyFile(destFile, srcFile string) error {
	src, err := ioutil.ReadFile(srcFile)
	if err != nil {
		return sqerr.NewInternal("Unable to read source file: " + err.Error())
	}
	err = ioutil.WriteFile(destFile, src, os.ModePerm)
	if err != nil {
		return sqerr.NewInternal("Unable to write file: " + err.Error())
	}
	return nil
}
func testRecoveryFunc(d RecoveryData) func(*testing.T) {
	return func(t *testing.T) {
		defer func() {
			r := recover()
			if d.ExpPanic && r == nil {
				t.Error(d.TestName + " did not panic")
			}
			if !d.ExpPanic && r != nil {
				t.Errorf(d.TestName + " panicked unexpectedly")
			}
		}()
		log.Warn(">>>" + d.TestName)
		var err error

		// Setup Files
		tmpDir, err := ioutil.TempDir("", "sqsrvtest")
		if err != nil {
			log.Fatal("Unable to setup tmp directory: ", err)
		}
		defer os.RemoveAll(tmpDir)

		// Setup files
		fileName = tmpDir + "/" + d.TransLogName
		recoveryFile = tmpDir + "/" + d.RecoveryLogName
		if d.CopyToTrans {
			err = copyFile(fileName, d.SourceFile)
			if err != nil {
				t.Fatal(err)
			}
		}
		if d.CopyToRecovery {
			err = copyFile(recoveryFile, d.SourceFile)
			if err != nil {
				t.Fatal(err)
			}

		}
		// clean up db
		tab := sqtables.GetTable(d.Profile, "names")
		if tab != nil {
			_, err = sqtables.DropTable(d.Profile, "names")
			if err != nil && err.Error() != "Invalid Name: Table names does not exist" {
				t.Fatal(err)
			}
			d.Profile.VerifyNoLocks()
		}
		if d.Started {
			logState.Start()
		} else {
			logState.Stop()
		}

		err = Recovery(d.Profile)
		if err != nil {
			log.Println(err.Error())
			if d.ExpErr == "" {
				t.Error(fmt.Sprintf("Unexpected Error in test: %s", err.Error()))
				return
			}
			if d.ExpErr != err.Error() {
				t.Error(fmt.Sprintf("Expecting Error %s but got: %s", d.ExpErr, err.Error()))
				return
			}
			return
		}
		if err == nil && d.ExpErr != "" {
			t.Error(fmt.Sprintf("Unexpected Success, should have returned error: %s", d.ExpErr))
			return
		}
		d.Profile.VerifyNoLocks()
	}
}
