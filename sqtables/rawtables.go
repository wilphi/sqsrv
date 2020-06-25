package sqtables

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/wilphi/converse/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

// CreateTableFromRaw creates a table and populates the data from a RawVals array
// It is assumed that the first row of the data are the column names
// Used for testing
func CreateTableFromRaw(profile *sqprofile.SQProfile, tableName string, rawData sqtypes.RawVals) (*TableRef, error) {
	if len(rawData) < 2 {
		return nil, sqerr.New("No data to create table, must include a col row and atleast one data row")
	}

	//Convert rawData to array of values
	data := sqtypes.CreateValuesFromRaw(rawData)

	// Take the first row and make it the col names for the table
	numCols := len(data[0])
	colDef := make([]column.Def, numCols)
	// set all colTypes to Null initially
	for i, col := range data[0] {
		colDef[i] = column.NewDef(col.String(), tokens.Null, false)
	}
	data = data[1:]

	// Validate the number of cols in the data and that they have consistent types
	for i, row := range data {
		if len(row) != numCols {
			return nil, sqerr.NewInternalf("Row #%d has %d values, it should have %d", i, len(row), numCols)
		}
		for j, val := range row {
			if val.IsNull() {
				//if the value is a null dont change the col type
				break
			}
			if colDef[j].ColType != val.Type() {
				if colDef[j].ColType == tokens.Null {
					colDef[j].ColType = val.Type()
				} else {
					return nil, sqerr.NewInternalf("Value[%d][%d] Type (%s) does not match the ColType of %s",
						i, j, tokens.IDName(val.Type()), tokens.IDName(colDef[j].ColType))
				}
			}
		}
	}

	tab := CreateTableDef(tableName, colDef)
	err := CreateTable(profile, tab)
	if err != nil {
		return nil, err
	}
	ds, err := NewDataSet(profile, NewTableListFromTableDef(profile, tab), ColsToExpr(column.NewListDefs(colDef)))
	if err != nil {
		return nil, err
	}

	ds.Vals = data

	_, err = tab.AddRows(profile, ds)
	return tab.TableRef(profile), err

}

// ReadRawFromFile reads raw table information from a file. Used for testing
func ReadRawFromFile(filePath string) (data sqtypes.RawVals, err error) {
	var line string

	file, err := os.Open(filePath)
	defer file.Close()

	if err != nil {
		return nil, err
	}

	// Start reading from the file with a reader.
	reader := bufio.NewReader(file)

	for row := 0; ; row++ {
		line, err = reader.ReadString('\n')

		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			err = nil
			break
		}

		newRow := make([]sqtypes.Raw, 0)
		//Parse line
		tkns := tokens.Tokenize(line)
		for {
			if tkns.IsEmpty() {
				break
			}
			tkn := tkns.Peek()

			switch tkn.ID() {
			case tokens.Quote:
				vtkn := tkn.(*tokens.ValueToken).Value()
				newRow = append(newRow, vtkn)
			case tokens.RWTrue:
				newRow = append(newRow, true)
			case tokens.RWFalse:
				newRow = append(newRow, false)
			case tokens.Num:
				vtkn := tkn.(*tokens.ValueToken).Value()
				r, err := convertStringToNum(vtkn, false)
				if err != nil {
					return nil, err
				}
				newRow = append(newRow, r)
			case tokens.Minus:
				tkns.Remove()
				tkn = tkns.Peek()
				if tkn.ID() == tokens.Num {
					vtkn := tkn.(*tokens.ValueToken).Value()
					r, err := convertStringToNum(vtkn, true)
					if err != nil {
						return nil, err
					}
					newRow = append(newRow, r)
				} else {
					return nil, sqerr.Newf("Unexpected Minus sign in line %d", row)
				}
			case tokens.Ident:
				vtkn := tkn.(*tokens.ValueToken).Value()
				if strings.ToLower(vtkn) == "nil" {
					newRow = append(newRow, nil)
				} else {
					return nil, sqerr.Newf("Unexpected Token %s in line %d", tkn.String(), row)
				}
			default:
				return nil, sqerr.Newf("Unexpected Token %s in line %d", tkn.String(), row)

			}

			tkns.Remove()
		}
		if len(newRow) == 0 {
			return nil, sqerr.Newf("Source file: %s can't contain blank lines", filePath)
		}

		data = append(data, newRow)
	}

	return data, err
}

func convertStringToNum(s string, isMinus bool) (sqtypes.Raw, error) {
	if strings.Contains(s, ".") {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return f, err
		}
		if isMinus {
			f = -f
		}
		return f, nil
	}

	i, err := strconv.Atoi(s)
	if err != nil {
		return i, err
	}
	if isMinus {
		i = -i
	}
	return i, nil
}

//CreateTableFromRawFile creates a table using a file in Raw format
func CreateTableFromRawFile(profile *sqprofile.SQProfile, filePath string, tableName string) (*TableRef, error) {
	rawVals, err := ReadRawFromFile(filePath)
	if err != nil {
		return nil, err
	}
	tref, err := CreateTableFromRaw(profile, tableName, rawVals)
	return tref, err
}
