package sqtables

import (
	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

// Expr constants for binary versions
const (
	IDValueExpr  = 200
	IDColExpr    = 201
	IDOpExpr     = 202
	IDCountExpr  = 203
	IDNegateExpr = 204
)

// Evaluate constants. Full means all parts must be valid to get a value, Partial means only parts that match current table matter
const (
	EvalFull    = false
	EvalPartial = true
)

// Expr interface maintains a tree structure of expressions that will eventually evaluate to a single value
type Expr interface {
	Left() Expr
	Right() Expr
	SetLeft(ex Expr)
	SetRight(ex Expr)
	ToString() string
	Name() string
	ColDef() ColDef
	ColDefs(tables ...*TableDef) []ColDef
	Evaluate(profile *sqprofile.SQProfile, partial bool, rows ...*RowDef) (sqtypes.Value, error)
	Reduce() (Expr, error)
	ValidateCols(profile *sqprofile.SQProfile, tables *TableList) error
	Encode() *sqbin.Codec
	Decode(*sqbin.Codec)
	SetAlias(alias string)
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

// ValueExpr stores a single value. It is a leaf node for the Expr tree
type ValueExpr struct {
	v     sqtypes.Value
	alias string
}

// Left - ValueExpr is a leaf node, it will always return nil
func (e *ValueExpr) Left() Expr {
	return nil
}

// Right - ValueExpr is a leaf node, it will always return nil
func (e *ValueExpr) Right() Expr {
	return nil
}

// SetLeft -
func (e *ValueExpr) SetLeft(ex Expr) {
	log.Panic("Invalid to SetLeft on a ValueExpr")
}

// SetRight -
func (e *ValueExpr) SetRight(ex Expr) {
	log.Panic("Invalid to SetRight on a ValueExpr")

}

// ToString - string representation of Expression. Will traverse to child conditions to form full string
func (e *ValueExpr) ToString() string {
	str := e.v.ToString()
	if e.alias != "" {
		str += " " + e.alias
	}
	return str
}

// Name returns the name of the expression
func (e *ValueExpr) Name() string {
	if e.alias != "" {
		return e.alias
	}
	return e.ToString()
}

// ColDef returns a column definition for the expression
func (e *ValueExpr) ColDef() ColDef {
	return ColDef{ColName: e.Name(), ColType: e.v.Type()}
}

// ColDefs returns a list of all actual columns in the expression
func (e *ValueExpr) ColDefs(tables ...*TableDef) []ColDef {
	return nil
}

// Evaluate -
func (e *ValueExpr) Evaluate(profile *sqprofile.SQProfile, partial bool, rows ...*RowDef) (sqtypes.Value, error) {
	return e.v, nil
}

// Reduce will colapse the expression to it's simplest form
func (e *ValueExpr) Reduce() (Expr, error) {
	return e, nil
}

// ValidateCols make sure that the cols in the expression match the tabledef
func (e *ValueExpr) ValidateCols(profile *sqprofile.SQProfile, tables *TableList) error {
	return nil
}

// NewValueExpr creates a new ValueExpr object
func NewValueExpr(v sqtypes.Value) Expr {
	return &ValueExpr{v: v}
}

// Encode returns a binary encoded version of the expression
func (e *ValueExpr) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	// Identify the type of Expression
	enc.Writebyte(IDValueExpr)

	enc.WriteString(e.alias)
	// Write out the value
	e.v.Write(enc)

	return enc
}

// Decode gets a binary encoded version of the expression
func (e *ValueExpr) Decode(dec *sqbin.Codec) {
	mkr := dec.Readbyte()
	if mkr != IDValueExpr {
		log.Panic("Found wrong statement type. Expecting IDValueExpr")
	}
	e.alias = dec.ReadString()
	e.v = sqtypes.ReadValue(dec)

}

//SetAlias sets an alternative name for the expression
func (e *ValueExpr) SetAlias(alias string) {
	e.alias = alias
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// ColExpr stores information about a column to allow Evaluate() to determine the correct Value
type ColExpr struct {
	col   ColDef
	alias string
}

// Left - ColExpr is a leaf node, it will always return nil
func (e *ColExpr) Left() Expr {
	return nil
}

// Right - ColExpr is a leaf node, it will always return nil
func (e *ColExpr) Right() Expr {
	return nil
}

// SetLeft -
func (e *ColExpr) SetLeft(ex Expr) {
	log.Panic("Invalid to SetLeft on a ValueExpr")
}

// SetRight -
func (e *ColExpr) SetRight(ex Expr) {
	log.Panic("Invalid to SetRight on a ValueExpr")

}

// ToString - string representation of Expression. Will traverse to child conditions to form full string
func (e *ColExpr) ToString() string {
	if e.alias == e.col.ColName {
		return e.alias
	}
	str := Ternary(e.col.TableName != "", e.col.TableName+".", "") + e.col.ColName
	//str := e.col.ColName
	if e.alias != "" {
		str += " " + e.alias
	}

	return str
}

// Name returns the name of the expression
func (e *ColExpr) Name() string {
	if e.alias != "" {
		return e.alias
	}
	return e.ToString()
}

// ColDef returns a column definition for the expression
func (e *ColExpr) ColDef() ColDef {
	return e.col
}

// ColDefs returns a list of all actual columns in the expression, filtered by the given tables
func (e *ColExpr) ColDefs(tables ...*TableDef) []ColDef {
	var ret []ColDef
	if tables == nil {
		return []ColDef{e.col}
	}
	for _, tab := range tables {
		if e.col.TableName == tab.tableName {
			ret = append(ret, e.col)
		}
	}
	return ret
}

// Evaluate -
func (e *ColExpr) Evaluate(profile *sqprofile.SQProfile, partial bool, rows ...*RowDef) (sqtypes.Value, error) {
	var row *RowDef

	// Find the row with the proper table name
	for _, rw := range rows {
		if e.col.TableName == rw.table.GetName(profile) {
			row = rw
			break
		}
	}
	if row == nil {
		// no table found
		if !partial {
			str := ""
			for _, rw := range rows {
				str += rw.table.GetName(profile) + ", "
			}
			return nil, sqerr.Newf("Column %q not found in Table(s): %s", e.col.ColName, str[:len(str)-2])
		}
		return nil, nil
	}
	rowValue, err := row.GetColData(profile, &e.col)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return rowValue, nil
}

// Reduce will colapse the expression to it's simplest form
func (e *ColExpr) Reduce() (Expr, error) {
	return e, nil
}

// ValidateCols make sure that the cols in the expression match the tabledef
func (e *ColExpr) ValidateCols(profile *sqprofile.SQProfile, tables *TableList) error {
	cd, err := tables.FindColDef(profile, e.col.ColName, e.col.TableName)
	if err != nil {
		return err
	}
	e.col = *cd

	return nil
}

// NewColExpr creates a new ColExpr object
func NewColExpr(c ColDef) Expr {
	return &ColExpr{col: c}

}

// Encode returns a binary encoded version of the expression
func (e *ColExpr) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	enc.Writebyte(IDColExpr)
	enc.WriteString(e.alias)

	// Write out the value
	e.col.Encode(enc)

	return enc
}

// Decode gets a binary encoded version of the expression
func (e *ColExpr) Decode(dec *sqbin.Codec) {
	mkr := dec.Readbyte()
	if mkr != IDColExpr {
		log.Panic("Found wrong statement type. Expecting ColExpr")
	}
	e.alias = dec.ReadString()
	e.col.Decode(dec)

}

//SetAlias sets an alternative name for the expression
func (e *ColExpr) SetAlias(alias string) {
	e.alias = alias
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// OpExpr allows for an operator to create a value based on two other values
type OpExpr struct {
	exL, exR Expr
	Operator string
	alias    string
}

// Left - returns the left expression for OpExpr
func (e *OpExpr) Left() Expr {
	return e.exL
}

// Right -returns the right expression for OpExpr
func (e *OpExpr) Right() Expr {
	return e.exR
}

// SetLeft -
func (e *OpExpr) SetLeft(ex Expr) {
	e.exL = ex
}

// SetRight -
func (e *OpExpr) SetRight(ex Expr) {
	e.exR = ex
}

// ToString - string representation of Expression. Will traverse to child conditions to form full string
func (e *OpExpr) ToString() string {
	str := "(" + e.exL.ToString() + e.Operator + e.exR.ToString() + ")"
	if e.alias != "" {
		str += " " + e.alias
	}
	return str
}

// Name returns the name of the expression
func (e *OpExpr) Name() string {
	if e.alias != "" {
		return e.alias
	}
	return e.ToString()
}

// ColDef returns a column definition for the expression
func (e *OpExpr) ColDef() ColDef {
	col := e.exL.ColDef()
	return ColDef{ColName: e.Name(), ColType: col.ColType}
}

// ColDefs returns a list of all actual columns in the expression
func (e *OpExpr) ColDefs(tables ...*TableDef) []ColDef {
	colsL := e.exL.ColDefs(tables...)
	colsR := e.exR.ColDefs(tables...)
	if colsL == nil {
		return colsR
	}
	if colsR == nil {
		return colsL
	}
	ret := append(colsL, colsR...)
	return ret
}

// Evaluate -
func (e *OpExpr) Evaluate(profile *sqprofile.SQProfile, partial bool, rows ...*RowDef) (sqtypes.Value, error) {

	boolresult := e.Operator == tokens.And || e.Operator == tokens.Or

	vL, err := e.exL.Evaluate(profile, partial, rows...)
	if err != nil {
		return nil, err
	}
	if vL == nil && !partial {
		return nil, nil
		//return nil, sqerr.Newf("Unable to evaluate %q", e.exL.Name())
	}

	vR, err := e.exR.Evaluate(profile, partial, rows...)
	if err != nil {
		return nil, err
	}
	if vR == nil && !partial {
		return nil, nil
		//		return nil, sqerr.Newf("Unable to evaluate %q", e.exR.Name())
	}

	if partial {
		if boolresult {
			if vL == nil {
				return vR, nil
			}
			if vR == nil {
				return vL, nil
			}
		}
		if vL == nil || vR == nil {
			return nil, nil
		}

	}

	return vL.Operation(e.Operator, vR)
}

// Reduce will colapse the expression to it's simplest form
func (e *OpExpr) Reduce() (Expr, error) {

	eL, err := e.exL.Reduce()
	if err != nil {
		return e, err
	}
	e.SetLeft(eL)
	vL, okL := eL.(*ValueExpr)

	eR, err := e.exR.Reduce()
	if err != nil {
		return e, err
	}
	e.SetRight(eR)
	vR, okR := eR.(*ValueExpr)

	if okL && okR {
		val, err := vL.v.Operation(e.Operator, vR.v)
		if err != nil {
			return e, err
		}
		return NewValueExpr(val), nil
	}
	return e, nil
}

// ValidateCols make sure that the cols in the expression match the tabledef
func (e *OpExpr) ValidateCols(profile *sqprofile.SQProfile, tables *TableList) error {
	err := e.exL.ValidateCols(profile, tables)
	if err != nil {
		return err
	}
	err = e.exR.ValidateCols(profile, tables)
	return err
}

// NewOpExpr creates a new OpExpr and returns it as an Expr
func NewOpExpr(exL Expr, op string, exR Expr) Expr {
	return &OpExpr{exL: exL, Operator: op, exR: exR}
}

// Encode returns a binary encoded version of the expression
func (e *OpExpr) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)

	// Identify the type of Expression
	enc.Writebyte(IDOpExpr)
	enc.WriteString(e.alias)

	enc.WriteString(e.Operator)

	tmp := e.exL.Encode()
	enc.Write(tmp.Bytes())

	tmp = e.exR.Encode()
	enc.Write(tmp.Bytes())

	return enc
}

// Decode gets a binary encoded version of the expression
func (e *OpExpr) Decode(dec *sqbin.Codec) {
	mkr := dec.Readbyte()
	if mkr != IDOpExpr {
		log.Panic("Found wrong statement type. Expecting IDOpExpr")
	}
	e.alias = dec.ReadString()
	e.Operator = dec.ReadString()
	e.exL = DecodeExpr(dec)

	e.exR = DecodeExpr(dec)

}

//SetAlias sets an alternative name for the expression
func (e *OpExpr) SetAlias(alias string) {
	e.alias = alias
}

///////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////////////////

// CountExpr stores information about a function to allow Evaluate() to determine the correct Value
type CountExpr struct {
	Cmd   string
	alias string
	cnt   int
}

// Left - CountExpr is a leaf node, it will always return nil
func (e *CountExpr) Left() Expr {
	return nil
}

// Right - CountExpr is a leaf node, it will always return nil
func (e *CountExpr) Right() Expr {
	return nil
}

// SetLeft -
func (e *CountExpr) SetLeft(ex Expr) {
	log.Panic("Invalid to SetLeft on a CountExpr")
}

// SetRight -
func (e *CountExpr) SetRight(ex Expr) {
	log.Panic("Invalid to SetRight on a CountExpr")

}

// ToString - string representation of Expression. Will traverse to child conditions to form full string
func (e *CountExpr) ToString() string {
	str := "count()"
	if e.alias != "" {
		str += " " + e.alias
	}
	return str
}

// Name returns the name of the expression
func (e *CountExpr) Name() string {
	if e.alias != "" {
		return e.alias
	}
	return e.ToString()
}

// ColDef returns a column definition for the expression
func (e *CountExpr) ColDef() ColDef {
	return ColDef{ColName: e.Name(), ColType: "INT"}
}

// ColDefs returns a list of all actual columns in the expression
func (e *CountExpr) ColDefs(tables ...*TableDef) []ColDef {
	return nil
}

// Evaluate -
func (e *CountExpr) Evaluate(profile *sqprofile.SQProfile, partial bool, rows ...*RowDef) (sqtypes.Value, error) {

	e.cnt++
	return nil, nil
}

// Reduce will colapse the expression to it's simplest form
func (e *CountExpr) Reduce() (Expr, error) {
	return e, nil
}

// ValidateCols make sure that the cols in the expression match the tabledef
func (e *CountExpr) ValidateCols(profile *sqprofile.SQProfile, tables *TableList) error {
	return nil
}

// NewCountExpr creates a new CountExpr object
func NewCountExpr() Expr {
	return &CountExpr{}

}

// Encode returns a binary encoded version of the expression
func (e *CountExpr) Encode() *sqbin.Codec {
	//enc := sqbin.NewCodec(nil)
	panic("CountExpr Encode not implemented")
	//return enc
}

// Decode gets a binary encoded version of the expression
func (e *CountExpr) Decode(*sqbin.Codec) {
	panic("CountExpr Decode not implemented")
}

//SetAlias sets an alternative name for the expression
func (e *CountExpr) SetAlias(alias string) {
	e.alias = alias
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// NegateExpr allows for an operator to create a value based on two other values
type NegateExpr struct {
	exL   Expr
	alias string
}

// Left - returns the left expression for NegateExpr
func (e *NegateExpr) Left() Expr {
	return e.exL
}

// Right -returns the right expression for NegateExpr
func (e *NegateExpr) Right() Expr {
	return nil
}

// SetLeft -
func (e *NegateExpr) SetLeft(ex Expr) {
	e.exL = ex
}

// SetRight -
func (e *NegateExpr) SetRight(ex Expr) {
	log.Panic("Invalid to SetRight on a NegateExpr")

}

// ToString - string representation of Expression. Will traverse to child conditions to form full string
func (e *NegateExpr) ToString() string {
	str := "(-" + e.exL.ToString() + ")"
	if e.alias != "" {
		str += " " + e.alias
	}
	return str
}

// Name returns the name of the expression
func (e *NegateExpr) Name() string {
	if e.alias != "" {
		return e.alias
	}
	return e.ToString()
}

// ColDef returns a column definition for the expression
func (e *NegateExpr) ColDef() ColDef {
	col := e.exL.ColDef()
	return ColDef{ColName: e.Name(), ColType: col.ColType}
}

// ColDefs returns a list of all actual columns in the expression
func (e *NegateExpr) ColDefs(tables ...*TableDef) []ColDef {
	colsL := e.exL.ColDefs(tables...)
	return colsL
}

// Evaluate -
func (e *NegateExpr) Evaluate(profile *sqprofile.SQProfile, partial bool, rows ...*RowDef) (sqtypes.Value, error) {
	var retVal sqtypes.Value

	vL, err := e.exL.Evaluate(profile, partial, rows...)
	if err != nil {
		return nil, err
	}
	if vL == nil {
		return nil, nil
		//return nil, sqerr.Newf("Unable to evaluate %q", e.exL.Name())
	}

	switch tp := vL.(type) {
	case sqtypes.SQInt:
		retVal = sqtypes.NewSQInt(-tp.Val)
	case sqtypes.SQFloat:
		retVal = sqtypes.NewSQFloat(-tp.Val)
	case sqtypes.SQNull:
		retVal = tp
	default:
		return vL, sqerr.NewSyntax("Only Int & Float values can be negated")
	}
	return retVal, nil
}

// Reduce will colapse the expression to it's simplest form
func (e *NegateExpr) Reduce() (Expr, error) {
	var retVal sqtypes.Value

	eL, err := e.exL.Reduce()
	if err != nil {
		return e, err
	}
	e.SetLeft(eL)
	vL, okL := eL.(*ValueExpr)

	if okL {
		val := vL.v
		switch tp := val.(type) {
		case sqtypes.SQInt:
			retVal = sqtypes.NewSQInt(-tp.Val)
		case sqtypes.SQFloat:
			retVal = sqtypes.NewSQFloat(-tp.Val)
		default:
			return vL, sqerr.NewSyntax("Only Int & Float values can be negated")
		}
		return NewValueExpr(retVal), nil
	}
	return e, nil
}

// ValidateCols make sure that the cols in the expression match the tabledef
func (e *NegateExpr) ValidateCols(profile *sqprofile.SQProfile, tables *TableList) error {
	err := e.exL.ValidateCols(profile, tables)

	return err
}

// NewNegateExpr creates a new NegateExpr and returns it as an Expr
func NewNegateExpr(exL Expr) Expr {
	return &NegateExpr{exL: exL}
}

// Encode returns a binary encoded version of the expression
func (e *NegateExpr) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	// Identify the type of Expression
	enc.Writebyte(IDNegateExpr)
	enc.WriteString(e.alias)

	tmp := e.exL.Encode()
	enc.Write(tmp.Bytes())

	return enc
}

// Decode gets a binary encoded version of the expression
func (e *NegateExpr) Decode(dec *sqbin.Codec) {
	mkr := dec.Readbyte()
	if mkr != IDNegateExpr {
		log.Panic("Found wrong statement type. Expecting IDNegateExpr")
	}
	e.alias = dec.ReadString()
	e.exL = DecodeExpr(dec)

}

//SetAlias sets an alternative name for the expression
func (e *NegateExpr) SetAlias(alias string) {
	e.alias = alias
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// FuncExpr stores information about a function to allow Evaluate() to determine the correct Value
type FuncExpr struct {
	Cmd   string
	exL   Expr
	alias string
}

// Left - FuncExpr is a leaf node, it will always return nil
func (e *FuncExpr) Left() Expr {
	return e.exL
}

// Right - FuncExpr is a leaf node, it will always return nil
func (e *FuncExpr) Right() Expr {
	return nil
}

// SetLeft -
func (e *FuncExpr) SetLeft(ex Expr) {
	e.exL = ex
}

// SetRight -
func (e *FuncExpr) SetRight(ex Expr) {
	log.Panic("Invalid to SetRight on a FuncExpr")

}

// ToString - string representation of Expression. Will traverse to child conditions to form full string
func (e *FuncExpr) ToString() string {
	str := e.Cmd + "(" + e.exL.ToString() + ")"
	if e.alias != "" {
		str += " " + e.alias
	}
	return str
}

// Name returns the name of the expression
func (e *FuncExpr) Name() string {
	if e.alias != "" {
		return e.alias
	}
	return e.ToString()
}

// ColDef returns a column definition for the expression
func (e *FuncExpr) ColDef() ColDef {
	return ColDef{ColName: e.Name(), ColType: "FUNC"}
}

// ColDefs returns a list of all actual columns in the expression
func (e *FuncExpr) ColDefs(tables ...*TableDef) []ColDef {
	colsL := e.exL.ColDefs(tables...)
	return colsL
}

// Evaluate takes the current Expression and calculates the results based on the given row
func (e *FuncExpr) Evaluate(profile *sqprofile.SQProfile, partial bool, rows ...*RowDef) (retVal sqtypes.Value, err error) {
	var vL sqtypes.Value

	vL, err = e.exL.Evaluate(profile, partial, rows...)
	if err != nil {
		return
	}
	if vL == nil {
		return nil, nil
		//return nil, sqerr.Newf("Unable to evaluate %q", e.exL.Name())
	}

	retVal, err = evalFunc(e.Cmd, vL)
	if err != nil {
		return
	}
	return
}

func evalFunc(cmd string, v sqtypes.Value) (retVal sqtypes.Value, err error) {

	switch cmd {
	case tokens.TypeFloat, tokens.TypeInt, tokens.TypeBool, tokens.TypeString:
		retVal, err = v.Convert(cmd)
	default:
		err = sqerr.NewSyntaxf("%q is not a valid function", cmd)
	}
	return
}

// Reduce will colapse the expression to it's simplest form
func (e *FuncExpr) Reduce() (Expr, error) {
	ex, err := e.exL.Reduce()
	if err != nil {
		return nil, err
	}
	e.exL = ex
	v, ok := ex.(*ValueExpr)
	if ok {
		val, err := evalFunc(e.Cmd, v.v)
		if err != nil {
			return nil, err
		}
		return NewValueExpr(val), nil
	}
	return e, nil
}

// ValidateCols make sure that the cols in the expression match the tabledef
func (e *FuncExpr) ValidateCols(profile *sqprofile.SQProfile, tables *TableList) error {
	return e.exL.ValidateCols(profile, tables)
}

// NewFuncExpr creates a new CountExpr object
func NewFuncExpr(cmd string, lExp Expr) Expr {
	return &FuncExpr{Cmd: cmd, exL: lExp}

}

// Encode returns a binary encoded version of the expression
func (e *FuncExpr) Encode() *sqbin.Codec {
	panic("FuncExpr Encode not implemented")
}

// Decode gets a binary encoded version of the expression
func (e *FuncExpr) Decode(*sqbin.Codec) {
	panic("FuncExpr Decode not implemented")
}

//SetAlias sets an alternative name for the expression
func (e *FuncExpr) SetAlias(alias string) {
	e.alias = alias
}

//////////////////////////////////////////////////////////////////////////////////////////////

//DecodeExpr returns an expression from an encoded one
func DecodeExpr(dec *sqbin.Codec) Expr {
	var ex Expr

	etype := dec.PeekByte()
	switch etype {
	case IDValueExpr:
		ex = &ValueExpr{}
	case IDColExpr:
		ex = &ColExpr{}
	case IDOpExpr:
		ex = &OpExpr{}
	case IDNegateExpr:
		ex = &NegateExpr{}
	case IDCountExpr:
		log.Panic("Unexpected Count expression in Decode")
	default:
		log.Panic("Unexpected expression type in Decode")

	}
	ex.Decode(dec)
	return ex
}
