package files

import (
	"fmt"
	"os"

	"github.com/wilphi/sqsrv/sqerr"
)

// Exists Tests to see if file exists and has a size >0
func Exists(fileName string) (bool, error) {
	exists := true
	_, err := os.Stat(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			exists = false
		} else {
			return false, err
		}
	}
	return exists, nil
}

//NumberFile renames a file (if it exists) by adding a number to the end of the name.
// if more files than maxFiles have been numbered then an error is returned
func NumberFile(fileName string, maxFiles int) error {
	isFile, err := Exists(fileName)
	if err != nil {
		return sqerr.Newf("Unable to rename file %s: %s", fileName, err.Error())
	}
	if isFile {
		fileChanged := false
		for i := 1; i < maxFiles; i++ {
			newFileName := fmt.Sprintf("%s-%d", fileName, i)
			isFile, err := Exists(newFileName)
			if isFile {
				continue
			}
			if err != nil {
				return sqerr.Newf("Unable to rename file %s to %s: %s", fileName, newFileName, err.Error())
			}
			err = os.Rename(fileName, newFileName)
			if err != nil {
				return sqerr.Newf("Unable to rename file %s to %s: %s", fileName, newFileName, err.Error())
			}
			fileChanged = true
			break
		}
		if !fileChanged {
			return sqerr.New("Unable to re-number file, It has been re-numbered to many times")
		}
	} else {
		return sqerr.New("Unable to renumber file, it does not exist")
	}
	return nil
}
