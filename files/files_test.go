package files_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/wilphi/sqsrv/files"
	"github.com/wilphi/sqsrv/sqtest"
)

type ExistsData struct {
	TestName    string
	FilePath    string
	ExpErr      string
	ExpectedRet bool
}

func testExistsFunc(d ExistsData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")

		ret, err := files.Exists(d.FilePath)
		if sqtest.CheckErr(t, err, d.ExpErr) {
			return
		}

		if ret != d.ExpectedRet {
			t.Errorf("The return value of %T does not match expected %T", ret, d.ExpectedRet)
			return
		}
	}
}
func TestGetIdentList(t *testing.T) {

	data := []ExistsData{
		{
			TestName:    "File Exists",
			FilePath:    "./test/exists.txt",
			ExpErr:      "",
			ExpectedRet: true,
		},
		{
			TestName:    "Zero len file",
			FilePath:    "./test/zero.txt",
			ExpErr:      "",
			ExpectedRet: true,
		},
		{
			TestName:    "File does not exist",
			FilePath:    "./test/notafile.txt",
			ExpErr:      "",
			ExpectedRet: false,
		},
		{
			TestName:    "Directory",
			FilePath:    "./test",
			ExpErr:      "",
			ExpectedRet: true,
		},
		{
			TestName:    "Directory2",
			FilePath:    "./test/not/",
			ExpErr:      "",
			ExpectedRet: false,
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testExistsFunc(row))

	}

}

type NumberFileData struct {
	TestName string
	FileName string
	ExpErr   string
	MaxFiles int
	Count    int
	NoCreate bool
}

func testNumberFileFunc(d NumberFileData) func(*testing.T) {
	return func(t *testing.T) {
		defer sqtest.PanicTestRecovery(t, "")
		// Setup test file
		tempdir, err := ioutil.TempDir("", "sqtest")
		if err != nil {
			t.Errorf("Error setting up test %s: %s", t.Name(), err)
		}
		defer os.RemoveAll(tempdir)

		path := tempdir + "/" + d.FileName

		for i := 1; i <= d.Count; i++ {
			if !d.NoCreate {
				_, err = os.Create(path)
				if err != nil {
					t.Errorf("Error setting up test %s: %s", t.Name(), err)
				}
			}
			newPath := fmt.Sprintf("%s/%s-%d", tempdir, d.FileName, i)

			err = files.NumberFile(path, d.MaxFiles)
			if sqtest.CheckErr(t, err, d.ExpErr) {
				return
			}

			FileExists, e := files.Exists(newPath)
			if e != nil {
				t.Errorf("Unexpected error for NewFile (%s): %s", newPath, e)
				return
			}
			if !FileExists {
				t.Errorf("NewFile (%s) was not created", newPath)
				return
			}
		}
	}
}
func TestNumberFile(t *testing.T) {

	data := []NumberFileData{
		{
			TestName: "Single renumber",
			FileName: "exists.txt",
			ExpErr:   "",
			MaxFiles: 3,
			Count:    1,
		},
		{
			TestName: "Multiple Renumbers",
			FileName: "exists.txt",
			ExpErr:   "",
			MaxFiles: 10,
			Count:    5,
		},
		/*	Known Bug work deferred
			{
				TestName: "Renumber past max",
				FileName: "exists.txt",
				ExpErr:   "Error: Unable to re-number file, It has been re-numbered to many times",
				MaxFiles: 2,
				Count:    4,
			},*/
		{
			TestName: "File Does Not Exist",
			FileName: "exists.txt",
			ExpErr:   "Error: Unable to renumber file, it does not exist",
			MaxFiles: 2,
			Count:    4,
			NoCreate: true,
		},
	}

	for i, row := range data {
		t.Run(fmt.Sprintf("%d: %s", i, row.TestName),
			testNumberFileFunc(row))

	}

}
