package sqtables

import (
	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/tokens"
)

/*

type Constraint struct {
	Type tokens.TokenID
	Name string
	Table *TableRef
	Cols SortOrder
}
*/

// Constraint is an interface for table constraints (Primary Key, Unique, Foreign Key, Index)
type Constraint interface {
	Type() tokens.TokenID
	Validate(profile *sqprofile.SQProfile, tab *TableDef) error
	String() string
	Ordering() int
}

////////////////////////////////////////////////////////////////////////////////////////////////////

// PrimaryKey structure holds the information for the PK constraint
type PrimaryKey struct {
	Table *TableDef
	Cols  SortOrder
	indx  *SQIndex
}

// Type returns the type of Constraint
func (c PrimaryKey) Type() tokens.TokenID {
	return tokens.Primary
}

// String returns string representation of the constraint
func (c PrimaryKey) String() string {
	s := "PRIMARY KEY " + c.Cols.String()

	return s
}

// Ordering returns an int used to sort a list of constraints
func (c PrimaryKey) Ordering() int {
	return 0
}

//Validate makes sure that the constraint is valid for the table
func (c PrimaryKey) Validate(profile *sqprofile.SQProfile, tab *TableDef) error {
	var err error

	if c.Table == tab {
		return nil
	}

	if c.Table != nil {
		return sqerr.NewInternalf("Primary Key definition is already attached to table %s", c.Table.tableName)
	}

	c.Table = tab

	// verify tab does not have more than one PK
	cnt := 0
	for _, con := range c.Table.constraints {
		if con.Type() == tokens.Primary {
			cnt++
			if cnt > 1 {
				return sqerr.NewSyntaxf("The table %s cannot have more than one Primary Key", c.Table.tableName)
			}
		}
	}

	// validate cols in sort order and dont allow nullable cols
	c.Cols, err = validateOrder(profile, "Primary Key", tab, c.Cols, true)

	if err != nil {
		return err
	}

	// create backing index

	c.indx, err = NewSQIndex(profile, c.Table.tableName+"_PK", tab, column.NewListNames(c.Cols.Names()), false, true)
	return err
}

//NewPrimaryKey create a new table constraint
func NewPrimaryKey(cols []string) Constraint {

	return &PrimaryKey{Cols: colsToOrderItem(cols)}
}

////////////////////////////////////////////////////////////////////////////////////////////////////

// ForeignKey structure holds the information for the FK constraint
type ForeignKey struct {
	Name  string
	Table *TableDef
	Cols  SortOrder
}

// Type returns the type of Constraint
func (c ForeignKey) Type() tokens.TokenID {
	return tokens.Foreign
}

// String returns string representation of the constraint
func (c ForeignKey) String() string {
	s := "FOREIGN KEY " + c.Cols.String()

	return s
}

// Ordering returns an int used to sort a list of constraints
func (c ForeignKey) Ordering() int {
	return 1
}

//Validate makes sure that the constraint is valid for the table
func (c ForeignKey) Validate(profile *sqprofile.SQProfile, tab *TableDef) error {
	if c.Table == tab {
		return nil
	}

	if c.Table != nil {
		return sqerr.NewInternalf("Foreign Key definition is already attached to table %s", c.Table.tableName)
	}

	log.Panic("Incomplete")
	return nil
}

//NewForeignKey create a new table constraint
func NewForeignKey(name string, cols []string) Constraint {
	return &ForeignKey{Name: name, Cols: colsToOrderItem(cols)}
}

////////////////////////////////////////////////////////////////////////////////////////////////////

// Unique structure holds the information for the Unique constraint
type Unique struct {
	Name  string
	Table *TableDef
	Cols  SortOrder
}

// Type returns the type of Constraint
func (c Unique) Type() tokens.TokenID {
	return tokens.Unique
}

// String returns string representation of the constraint
func (c Unique) String() string {
	s := "UNIQUE " + c.Cols.String()

	return s
}

// Ordering returns an int used to sort a list of constraints
func (c Unique) Ordering() int {
	return 2
}

//Validate makes sure that the constraint is valid for the table
func (c Unique) Validate(profile *sqprofile.SQProfile, tab *TableDef) error {
	var err error

	if c.Table == tab {
		return nil
	}

	if c.Table != nil {
		return sqerr.NewInternalf("Unique definition is already attached to table %s", c.Table.tableName)
	}

	// validate cols in sort order and dont allow nullable cols
	c.Cols, err = validateOrder(profile, "Primary Key", tab, c.Cols, true)

	return err
}

//NewUnique create a new table constraint
func NewUnique(name string, cols []string) Constraint {
	return &Unique{Name: name, Cols: colsToOrderItem(cols)}
}

////////////////////////////////////////////////////////////////////////////////////////////////////

// Index structure holds the information for and Index
type Index struct {
	Table *TableDef
	Cols  SortOrder
}

// Type returns the type of Constraint
func (c Index) Type() tokens.TokenID {
	return tokens.Unique
}

// String returns string representation of the constraint
func (c Index) String() string {
	s := "INDEX " + c.Cols.String()

	return s
}

// Ordering returns an int used to sort a list of constraints
func (c Index) Ordering() int {
	return 3
}

//Validate makes sure that the constraint is valid for the table
func (c Index) Validate(profile *sqprofile.SQProfile, tab *TableDef) error {
	if c.Table == tab {
		return nil
	}

	if c.Table != nil {
		return sqerr.NewInternalf("Unique definition is already attached to table %s", c.Table.tableName)
	}

	log.Panic("Incomplete")
	return nil
}

//NewIndex create a new table constraint
func NewIndex(name string, cols []string) Constraint {

	log.Panic("Incomplete")
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////

// Constraints is a list of constraints
type Constraints []Constraint

/*
// AddRow adds rows to the constraints
func (cons Constraints) AddRow(profile *sqprofile.SQProfile, row RowInterface) error {
	for _,con:=range cons {
		err := con.AddRow(profile *sqprofile.SQProfile, row RowInterface)
		if err!=nil {
			return err
		}
	}
	return nil
}

*/
////////////////////////////////////////////////////////////////////////////////////////////////////

func colsToOrderItem(cols []string) SortOrder {
	var colOrder SortOrder
	colOrder = make(SortOrder, len(cols))
	for i, col := range cols {
		colOrder[i].ColName = col
		colOrder[i].SortType = tokens.Asc
	}
	return colOrder
}

func validateOrder(profile *sqprofile.SQProfile, constraintName string, tab *TableDef, order SortOrder, noNulls bool) (SortOrder, error) {
	for x, col := range order {
		//set the index
		cd := tab.FindColDef(profile, col.ColName)
		if cd == nil {
			// Col not found
			return nil, sqerr.Newf("Column %s not found in table %s for %s", col.ColName, tab.tableName, constraintName)
		}
		order[x].idx = cd.Idx
		if !cd.IsNotNull && noNulls {
			return nil, sqerr.NewSyntaxf("Column %s must not allow NULLs for %s", cd.ColName, constraintName)
		}

	}
	return order, nil
}

/*
func orderItemstring(Cols SortOrder) string {
	if len(Cols) == 0 {
		return ""
	}
	s := "("
	for _, col := range Cols {
		s += col.ColName
		if col.SortType == tokens.Desc {
			s += " DESC"
		}
		s += ", "
	}

	return s[:len(s)-2] + ")"
}
*/
