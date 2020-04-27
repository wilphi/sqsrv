package redo

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqptr"
	"github.com/wilphi/sqsrv/sqtest"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
	"github.com/wilphi/sqsrv/transid"

	"github.com/wilphi/sqsrv/sqtables"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/sqerr"
)

const (
	TMTestStmt    = 250
	TMTestWithErr = 251
)

var Items []string

// TestStmt -
type TestStmt struct {
	Str string
}

func init() {
	DecodeStatementHook = func(tm sqbin.TypeMarker) LogStatement {
		var stmt LogStatement
		switch tm {
		case TMTestStmt:
			stmt = &TestStmt{}
		case TMTestWithErr:
			stmt = &TestWithErr{}
		default:
			panic(fmt.Sprintf("Unknown LogStatement Type %d-%s", tm, sqbin.TMToString(tm)))
		}
		return stmt
	}

	sqbin.RegisterType("TMTestStmt", TMTestStmt)
	sqbin.RegisterType("TMTestWithErr", TMTestWithErr)
}

// Encode uses sqbin.Codec to return a binary encoded version of the statement
func (t *TestStmt) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	// Identify the type of logstatment
	enc.WriteTypeMarker(TMTestStmt)
	// Id of transaction statement
	enc.WriteString(t.Str)
	return enc
}

// Decode uses sqbin.Codec to return a binary encoded version of the statement
func (t *TestStmt) Decode(dec *sqbin.Codec) {

	dec.ReadTypeMarker(TMTestStmt)

	t.Str = dec.ReadString()

}

// Recreate for TestStmt
func (t *TestStmt) Recreate(profile *sqprofile.SQProfile) error {
	Items = append(Items, "Recreated "+t.Str)
	return nil
}

// Identify for TestStmt
func (t *TestStmt) Identify(ID uint64) string {
	return "Test Statement: " + t.Str
}

// TestWithErr - for testing error paths in software
type TestWithErr struct {
	Str string
}

// Encode uses sqbin.Codec to return a binary encoded version of the statement
func (t *TestWithErr) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	// Identify the type of logstatment
	enc.WriteTypeMarker(TMTestWithErr)

	enc.WriteString(t.Str)
	return enc
}

// Decode uses sqbin.Codec to return a binary encoded version of the statement
func (t *TestWithErr) Decode(dec *sqbin.Codec) {
	dec.ReadTypeMarker(TMTestWithErr)

	t.Str = dec.ReadString()

}

// Recreate TestWithErr for testing error paths
func (t *TestWithErr) Recreate(profile *sqprofile.SQProfile) error {
	Items = append(Items, "Not Recreated "+t.Str)

	return sqerr.NewInternal("Testing Error Handling: " + t.Str)
}

// Recreate TestWithErr for testing error paths
func (t *TestWithErr) Identify(ID uint64) string {
	return "Test With Error: " + t.Str
}

///////////////////////////////////////////////////////////////////////////////////
// Mock for io.Reader/io.Writer

type IORWErr struct {
	Buff     bytes.Buffer
	ErrAfter int
	Skipcnt  int
	Err      string
}

func (i *IORWErr) Write(p []byte) (n int, err error) {
	if i.Err != "" && i.ErrAfter <= i.Skipcnt {
		return 0, errors.New(i.Err)
	}
	i.Skipcnt++
	return i.Buff.Write(p)
}

func (i *IORWErr) Read(p []byte) (n int, err error) {
	if i.Err != "" && i.ErrAfter <= i.Skipcnt {
		if i.Err == "EOF" {
			return 0, io.EOF
		}
		return 0, errors.New(i.Err)
	}
	i.Skipcnt++
	return i.Buff.Read(p)
}

/////////////////////////////////////////////////////////////////////////

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
		{"CreateDDL is a LogStatement", &CreateDDL{}},
		{"InsertRows is a LogStatement", &InsertRows{}},
		{"UpdateRows is a LogStatement", &UpdateRows{}},
		{"DeleteRows is a LogStatement", &DeleteRows{}},
		{"DropDDL is a LogStatement", &DropDDL{}},
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
		defer sqtest.PanicTestRecovery(t, "")

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
			Stmts:    []LogStatement{&TestStmt{"Test0"}, &TestStmt{"Test1"}, &TestStmt{"Test2"}, &TestStmt{"Test3"}, &TestStmt{"Test4"}},
			ErrAfter: -1,
			ExpErr:   "",
			ExpItems: []string{"Recreated Test0", "Recreated Test1", "Recreated Test2", "Recreated Test3", "Recreated Test4"},
		},
		{
			TestName: "Error Recreating",
			Stmts:    []LogStatement{&TestStmt{"ErrTest0"}, &TestWithErr{"ErrTest1"}},
			ErrAfter: -1,
			ExpErr:   "Internal Error: Testing Error Handling: ErrTest1",
			ExpItems: []string{"Recreated ErrTest0", "Not Recreated ErrTest1"},
		},
		{
			TestName: "Error Reading Log",
			Stmts:    []LogStatement{&TestStmt{"Test0"}, &TestStmt{"Test1"}, &TestStmt{"Test2"}},
			ErrAfter: 2,
			ExpPanic: "Type marker did not match expected: Actual = 67-TMString, Expected = 64-TMUint64",
			ExpErr:   "",
			ExpItems: []string{"Recreated Test0", "Recreated Test1"},
		},
		{
			TestName: "Recreate with skip",
			Stmts:    []LogStatement{&TestStmt{"Test0"}, &TestStmt{"Test1"}, &TestStmt{"Test2"}, &TestStmt{"Test3"}, &TestStmt{"Test4"}},
			ErrAfter: -1,
			ExpErr:   "",
			ExpItems: []string{"Recreated Test2", "Recreated Test3", "Recreated Test4"},
			IDStart:  2,
		},
		{
			TestName:   "IO Error 1",
			Stmts:      []LogStatement{&TestStmt{"Test0"}, &TestStmt{"Test1"}, &TestStmt{"Test2"}, &TestStmt{"Test3"}, &TestStmt{"Test4"}},
			ErrAfter:   -1,
			ExpErr:     "Read Error in Read #1",
			ExpItems:   []string{"Recreated Test0", "Recreated Test1", "Recreated Test2", "Recreated Test3", "Recreated Test4"},
			IOErrAfter: 0,
			IOErrMsg:   "Read Error in Read #1",
		},
		{
			TestName:   "IO Error 2",
			Stmts:      []LogStatement{&TestStmt{"Test0"}, &TestStmt{"Test1"}, &TestStmt{"Test2"}, &TestStmt{"Test3"}, &TestStmt{"Test4"}},
			ErrAfter:   -1,
			ExpErr:     "Read Error in Read #2",
			ExpItems:   []string{"Recreated Test0", "Recreated Test1", "Recreated Test2", "Recreated Test3", "Recreated Test4"},
			IOErrAfter: 1,
			IOErrMsg:   "Read Error in Read #2",
		},
		{
			TestName:   "IO Error 3",
			Stmts:      []LogStatement{&TestStmt{"Test0"}, &TestStmt{"Test1"}, &TestStmt{"Test2"}, &TestStmt{"Test3"}, &TestStmt{"Test4"}},
			ErrAfter:   -1,
			ExpErr:     "Read Error in Read #3",
			ExpItems:   []string{"Recreated Test0", "Recreated Test1", "Recreated Test2", "Recreated Test3", "Recreated Test4"},
			IOErrAfter: 2,
			IOErrMsg:   "Read Error in Read #3",
		},
		{
			TestName:   "IO Error EOF",
			Stmts:      []LogStatement{&TestStmt{"Test0"}, &TestStmt{"Test1"}, &TestStmt{"Test2"}, &TestStmt{"Test3"}, &TestStmt{"Test4"}},
			ErrAfter:   -1,
			ExpErr:     "",
			ExpItems:   []string{"Recreated Test0", "Recreated Test1", "Recreated Test2", "Recreated Test3"},
			IOErrAfter: 12,
			IOErrMsg:   "EOF",
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testReadTlogFunc(profile, row))

	}
}

type TestData struct {
	TestName   string
	Stmts      []LogStatement
	ExpItems   []string
	ExpErr     string
	ErrAfter   int
	ExpPanic   string
	IDStart    uint64
	IOErrAfter int
	IOErrMsg   string
}

func testReadTlogFunc(profile *sqprofile.SQProfile, d TestData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)
		var err error
		transid.SetTransID(d.IDStart)
		Items = []string{}
		enc := sqbin.NewCodec(nil)
		for i, stmt := range d.Stmts {
			if i == d.ErrAfter {
				enc.WriteString("Test Object")
			} else {
				tmpenc := stmt.Encode()

				// TransID must start at 1
				enc.WriteUint64(uint64(i + 1))
				enc.WriteInt64(int64(tmpenc.Len()))
				enc.Write(tmpenc.Bytes())
			}
			if err != nil {
				t.Fatal("Unexpected Error while encoding during test: ", err)
			}
		}
		b := enc.Bytes()
		//file := bytes.NewBuffer(b)
		file := &IORWErr{}
		file.Buff.Write(b)
		file.Err = d.IOErrMsg
		file.ErrAfter = d.IOErrAfter

		err = ReadTlog(profile, file)
		if sqtest.CheckErr(t, err, d.ExpErr) {
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
		{
			TestName:     "Logging started",
			TransLogName: "transaction.tlog",
			Started:      true,
			ExpPanic:     "Recovery must occur before the transaction log has been started",
			ExpErr:       "",
			CreateTrans:  false,
			Profile:      sqprofile.CreateSQProfile(),
		},
		{
			TestName:     "No Logs",
			TransLogName: "transaction.tlog",
			Started:      false,
			ExpPanic:     "",
			ExpErr:       "",
			CreateTrans:  false,
			Profile:      sqprofile.CreateSQProfile(),
		},

		{
			TestName:     "Trans only Log",
			TransLogName: "transaction.tlog",
			Started:      false,
			CreateTrans:  true,
			ExpPanic:     "",
			ExpErr:       "",
			Profile:      sqprofile.CreateSQProfile(),
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testRecoveryFunc(row))

	}

}

type RecoveryData struct {
	TestName     string
	TransLogName string
	SourceFile   string
	CreateTrans  bool
	Started      bool
	ExpPanic     string
	ExpErr       string
	Profile      *sqprofile.SQProfile
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
		defer sqtest.PanicTestRecovery(t, d.ExpPanic)
		var err error

		// Setup Files
		tmpDir, err := ioutil.TempDir("", "sqsrvtest")
		if err != nil {
			log.Fatal("Unable to setup tmp directory: ", err)
		}
		defer os.RemoveAll(tmpDir)

		// Setup files
		logFileName = tmpDir + "/" + d.TransLogName
		tableName := "tlogtest"
		if d.CreateTrans {
			err = createTransLog(logFileName, tableName)
			if err != nil {
				t.Error("Unable to create transaction log file")
			}
		}

		// clean up db
		tab, err := sqtables.GetTable(d.Profile, tableName)
		if err != nil {
			t.Error(err)
			return
		}

		if tab != nil {
			err = sqtables.DropTable(d.Profile, tableName)
			if err != nil && err.Error() != "Invalid Name: Table names does not exist" {
				t.Error(err)
			}
			d.Profile.VerifyNoLocks()
		}
		if d.Started {
			logState.Start()
		} else {
			logState.Stop()
		}
		transid.SetTransID(0)
		err = Recovery(d.Profile)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		d.Profile.VerifyNoLocks()
	}
}

func createTransLog(testFileName string, tableName string) error {

	data := []LogStatement{
		NewCreateDDL(tableName, []sqtables.ColDef{sqtables.NewColDef("col1", tokens.Int, false)}),
		NewInsertRows(tableName, []string{"col1"}, sqtypes.CreateValuesFromRaw(sqtypes.RawVals{{1}, {2}, {3}}), sqptr.SQPtrs{1, 2, 3}),
		NewInsertRows(tableName, []string{"col1"}, sqtypes.CreateValuesFromRaw(sqtypes.RawVals{{4}, {5}, {6}}), sqptr.SQPtrs{4, 5, 6}),
		NewInsertRows(tableName, []string{"col1"}, sqtypes.CreateValuesFromRaw(sqtypes.RawVals{{7}, {8}, {9}}), sqptr.SQPtrs{7, 8, 9}),
		NewInsertRows(tableName, []string{"col1"}, sqtypes.CreateValuesFromRaw(sqtypes.RawVals{{10}, {11}, {12}}), sqptr.SQPtrs{10, 11, 12}),
	}

	// If the file doesn't exist, create it. Append to the file as write only
	file, err := os.OpenFile(testFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		//	log.Fatal(err)
		log.Panic(err)
	}

	defer file.Close()
	for id, stmt := range data {
		// set the transaction log ID
		//		stmt.SetID(uint64(id + 10))

		encStmt := stmt.Encode()
		encStmt.Insert(uint64(id+10), int64(encStmt.Len()))

		_, err := file.Write(encStmt.Bytes())
		// If there was an error put it on the respond channel from the sender
		if err != nil {
			return err
		}

	}
	return nil

}

func TestTransProc(t *testing.T) {
	t.Run("File Error", func(t *testing.T) {
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("%s did not panic", t.Name())
				return
			}
			str := ""
			switch o := r.(type) {
			case string:
				str = o
			case *log.Entry:
				str = o.Message
			default:
				str = "Unknown type in recovery"
			}

			expErr := "no such file or directory"
			if !strings.Contains(str, expErr) {
				t.Errorf("%s: Actual Error %q does not match expected %q", t.Name(), str, expErr)
				return
			}
		}()
		logFileName = "./notadirectory/transaction.tlog"
		transProc()

	})

	// Setup Files
	tmpDir, err := ioutil.TempDir("", "sqsrvtest")
	if err != nil {
		log.Fatal("Unable to setup tmp directory: ", err)
	}
	defer os.RemoveAll(tmpDir)

	t.Run("Verify Tlog file", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		expTlog := tmpDir + "/" + "transaction.tlog"
		SetTLog(expTlog)

		if logFileName != expTlog {
			t.Errorf("%s: Actual transaction log file name %q does not match expected %q", t.Name(), logFileName, expTlog)
		}
	})

	t.Run("Double set Tlog file", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		expTlog := tmpDir + "/" + "transaction2.tlog"
		SetTLog(expTlog)

		if logFileName == expTlog {
			t.Errorf("%s: Actual transaction log file name %q must not match %q", t.Name(), logFileName, expTlog)
		}
	})
	tlog = make(TChan, 10)
	logState.Start()

	t.Run("Double Start", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "transProc is already running")

		Start()
		runtime.Gosched()
		transProc()
	})
	t.Run("Send", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		stmt := &TestStmt{"Test0"}
		err := Send(stmt)
		sqtest.CheckErr(t, err, "")
	})
	t.Run("Stop", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")
		//pause for a bit
		time.Sleep(5 * time.Millisecond)
		Stop()
		if !atomic.CompareAndSwapUint64(&guardTransProc, 0, 0) {
			t.Error("transProc has not stopped")
		}
	})
	t.Run("Send while stopped", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		stmt := &TestStmt{"Test0"}
		err := Send(stmt)
		sqtest.CheckErr(t, err, "")
	})
	t.Run("LazyLog", func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")
		tlog = make(TChan, 10)

		SetLazyTlog(10)
		Start()

		err := Send(&TestStmt{"LazyTest0"})
		sqtest.CheckErr(t, err, "")
		err = Send(&TestStmt{"LazyTest1"})
		sqtest.CheckErr(t, err, "")
		err = Send(&TestStmt{"LazyTest2"})
		sqtest.CheckErr(t, err, "")
		err = Send(&TestStmt{"LazyTest3"})
		time.Sleep(20 * time.Millisecond)
		sqtest.CheckErr(t, err, "")
		err = Send(&TestStmt{"LazyTest4"})
		sqtest.CheckErr(t, err, "")
		err = Send(&TestStmt{"LazyTest5"})
		sqtest.CheckErr(t, err, "")

	})
}
