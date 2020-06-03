package sqtables

import (
	"sort"
	"strings"

	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtables/column"
	"github.com/wilphi/sqsrv/sqtables/moniker"
)

//TableList holds a unique list of tables listed in From Clause of query
type TableList struct {
	tables map[string]*TableRef
}

// FindTableDef - Find a table def given the table name/alias
func (tl *TableList) FindTableDef(profile *sqprofile.SQProfile, name string) *TableDef {
	ft, ok := tl.tables[strings.ToLower(name)]
	if !ok {
		return nil
	}
	return ft.Table
}

// String - generates a string containing the names of the tables in the list
func (tl *TableList) String(profile *sqprofile.SQProfile) string {
	names := ""
	for _, n := range tl.TableNames() {
		names += n.Show() + ", "
	}
	if names != "" {
		names = names[:len(names)-2]
	}
	return names
}

//FindDef - Finds a column.Def based on colName and tableAlias. If the tableAlias is empty, it will look in all
//   tables in the list for the column. An error will occur if the col is found in multiple tables
func (tl *TableList) FindDef(profile *sqprofile.SQProfile, colName, tableAlias string) (col *column.Def, err error) {

	if tableAlias == "" {
		found := false
		for _, ft := range tl.tables {
			if ft.Table == nil {
				return nil, sqerr.NewInternalf("Table %s does not have a TableDef assigned", ft.Name.Show())
			}
			cd := ft.Table.FindColDef(profile, colName)
			if cd != nil {
				if found {
					// has been found before
					return nil, sqerr.Newf("Column %q found in multiple tables, add tablename to differentiate", colName)
				}
				col = cd
				found = true
			}
		}
		if !found {
			return nil, sqerr.Newf("Column %q not found in Table(s): %s", colName, tl.String(profile))
		}
	} else {
		tabF, ok := tl.tables[strings.ToLower(tableAlias)]
		if !ok {
			return nil, sqerr.Newf("Table %s not found in table list", tableAlias)
		}
		tab := tabF.Table
		if tab == nil {
			return nil, sqerr.NewInternalf("Table %s does not have a TableDef", tableAlias)
		}
		col = tab.FindColDef(profile, colName)
		if col == nil {
			return nil, sqerr.Newf("Column %q not found in Table %q", colName, tableAlias)
		}
	}
	return col, nil
}

// Len returns number of tables in list
func (tl *TableList) Len() int {
	return len(tl.tables)
}

// Add  a new table to the list. Will return an error if a duplicate
// tableName/Alias pair is added. If the TableDef is not provided it will be found
func (tl *TableList) Add(profile *sqprofile.SQProfile, ft TableRef) error {
	var err error

	err = ft.Validate(profile)
	if err != nil {
		return err
	}

	// if there is an alias use it as key instead of tableName
	key := strings.ToLower(ft.Name.Show())

	//See if key has already been used
	_, ok := tl.tables[key]
	if ok {
		return sqerr.Newf("Duplicate table name/alias %q", key)
	}
	tl.tables[key] = &ft

	return nil
}

// AllCols returns an array of all cols in the tables of the tablelist as ColDefs
func (tl *TableList) AllCols(profile *sqprofile.SQProfile) []column.Ref {

	var cols []column.Ref
	displayTName := tl.Len() > 1
	colm := make(map[column.Ref]bool)
	for _, tab := range tl.tables {
		tc := tab.Table.GetCols(profile)
		alias := tab.Name.Alias
		for _, cd := range tc.GetRefs() {
			cd.DisplayTableName = displayTName
			// make sure that the table alias in the tablelist is added to the Col Ref
			cd.TableName.Alias = alias
			colm[cd] = true
		}
	}
	for key := range colm {
		cols = append(cols, key)
	}
	return cols
}

// RLock read locks all tables in the list
func (tl *TableList) RLock(profile *sqprofile.SQProfile) error {
	var err error
	var locklist []*TableDef

	for _, tab := range tl.tables {
		err = tab.Table.RLock(profile)
		if err != nil {
			for _, tDef := range locklist {
				tDef.RUnlock(profile)
			}
			return err
		}
		locklist = append(locklist, tab.Table)
	}
	return nil
}

// RUnlock unloack read lock from all tables in list
func (tl *TableList) RUnlock(profile *sqprofile.SQProfile) {
	for _, tab := range tl.tables {
		tab.Table.RUnlock(profile)
	}
}

// TableNames returns the tablenames of the TableList
func (tl *TableList) TableNames() []*moniker.Moniker {
	names := make([]*moniker.Moniker, len(tl.tables))
	i := 0
	for _, tab := range tl.tables {

		names[i] = tab.Name
		i++
	}
	sort.Slice(names, func(i int, j int) bool { return names[i].Show() < names[j].Show() })
	return names
}

// NewTableList - Initialize a new TableList
func NewTableList(profile *sqprofile.SQProfile, tables []TableRef) *TableList {
	tl := TableList{tables: make(map[string]*TableRef)}
	for _, ft := range tables {
		tl.Add(profile, ft)
	}
	return &tl
}

// NewTableListFromTableDef - Initialize a new TableList
func NewTableListFromTableDef(profile *sqprofile.SQProfile, tabs ...*TableDef) *TableList {
	tl := TableList{tables: make(map[string]*TableRef)}
	for _, tab := range tabs {
		ft := TableRef{Name: moniker.New(tab.GetName(profile), ""), Table: tab}
		tl.Add(profile, ft)
	}

	return &tl
}

// Ternary is an implementation of a ternary operator for strings
func Ternary(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}
