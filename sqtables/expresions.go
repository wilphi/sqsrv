package sqtables

import (
	"fmt"

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

// Expr interface maintains a tree structure of expressions that will eventually evaluate to a single value
type Expr interface {
	GetLeft() Expr
	GetRight() Expr
	SetLeft(ex Expr)
	SetRight(ex Expr)
	ToString() string
	GetName() string
	GetColDef() ColDef
	Evaluate(profile *sqprofile.SQProfile, row *RowDef) (sqtypes.Value, error)
	Reduce() (Expr, error)
	ValidateCols(profile *sqprofile.SQProfile, tab *TableDef) error
	Encode() *sqbin.Codec
	Decode(*sqbin.Codec)
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

// ValueExpr stores a single value. It is a leaf node for the Expr tree
type ValueExpr struct {
	v    sqtypes.Value
	name string
}

// GetLeft - ValueExpr is a leaf node, it will always return nil
func (e *ValueExpr) GetLeft() Expr {
	return nil
}

// GetRight - ValueExpr is a leaf node, it will always return nil
func (e *ValueExpr) GetRight() Expr {
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
	return e.v.ToString()
}

// GetName returns the name of the expression
func (e *ValueExpr) GetName() string {
	return e.name
}

// GetColDef returns a column definition for the expression
func (e *ValueExpr) GetColDef() ColDef {
	return ColDef{ColName: e.GetName(), ColType: e.v.GetType()}
}

// Evaluate -
func (e *ValueExpr) Evaluate(profile *sqprofile.SQProfile, row *RowDef) (sqtypes.Value, error) {
	return e.v, nil
}

// Reduce will colapse the expression to it's simplest form
func (e *ValueExpr) Reduce() (Expr, error) {
	return e, nil
}

// ValidateCols make sure that the cols in the expression match the tabledef
func (e *ValueExpr) ValidateCols(profile *sqprofile.SQProfile, tab *TableDef) error {
	return nil
}

// NewValueExpr creates a new ValueExpr object
func NewValueExpr(v sqtypes.Value) Expr {
	return &ValueExpr{v: v, name: v.ToString()}
}

// Encode returns a binary encoded version of the expression
func (e *ValueExpr) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	// Identify the type of Expression
	enc.Writebyte(IDValueExpr)

	enc.WriteString(e.name)
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
	e.name = dec.ReadString()
	e.v = sqtypes.ReadValue(dec)

}

///////////////////////////////////////////////////////////////////////////////////////////////////

// ColExpr stores information about a column to allow Evaluate() to determine the correct Value
type ColExpr struct {
	col  ColDef
	name string
}

// GetLeft - ColExpr is a leaf node, it will always return nil
func (e *ColExpr) GetLeft() Expr {
	return nil
}

// GetRight - ColExpr is a leaf node, it will always return nil
func (e *ColExpr) GetRight() Expr {
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
	return e.col.ColName + "[" + e.col.ColType + "]"
}

// GetName returns the name of the expression
func (e *ColExpr) GetName() string {
	return e.name
}

// GetColDef returns a column definition for the expression
func (e *ColExpr) GetColDef() ColDef {
	return e.col
}

// Evaluate -
func (e *ColExpr) Evaluate(profile *sqprofile.SQProfile, row *RowDef) (sqtypes.Value, error) {
	// Get row value
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
func (e *ColExpr) ValidateCols(profile *sqprofile.SQProfile, tab *TableDef) error {
	cd := tab.FindColDef(profile, e.col.ColName)
	if cd == nil {
		return sqerr.New(fmt.Sprintf("Table %s does not have a column named %s", tab.GetName(profile), e.col.ColName))
	}
	e.col = *cd

	return nil
}

// NewColExpr creates a new ColExpr object
func NewColExpr(c ColDef) Expr {
	return &ColExpr{col: c, name: c.ColName}

}

// Encode returns a binary encoded version of the expression
func (e *ColExpr) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	enc.Writebyte(IDColExpr)
	enc.WriteString(e.name)

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
	e.name = dec.ReadString()
	e.col.Decode(dec)

}

///////////////////////////////////////////////////////////////////////////////////////////////////

// OpExpr allows for an operator to create a value based on two other values
type OpExpr struct {
	exL, exR Expr
	Operator string
	name     string
}

// GetLeft - returns the left expression for OpExpr
func (e *OpExpr) GetLeft() Expr {
	return e.exL
}

// GetRight -returns the right expression for OpExpr
func (e *OpExpr) GetRight() Expr {
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
	return "(" + e.exL.ToString() + e.Operator + e.exR.ToString() + ")"
}

// GetName returns the name of the expression
func (e *OpExpr) GetName() string {
	return e.name
}

// GetColDef returns a column definition for the expression
func (e *OpExpr) GetColDef() ColDef {
	col := e.exL.GetColDef()
	return ColDef{ColName: e.GetName(), ColType: col.ColType}
}

// Evaluate -
func (e *OpExpr) Evaluate(profile *sqprofile.SQProfile, row *RowDef) (sqtypes.Value, error) {
	vL, err := e.exL.Evaluate(profile, row)
	if err != nil {
		return nil, err
	}
	vR, err := e.exR.Evaluate(profile, row)
	if err != nil {
		return nil, err
	}

	return vL.MathOp(e.Operator, vR)
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
		val, err := vL.v.MathOp(e.Operator, vR.v)
		if err != nil {
			return e, err
		}
		return NewValueExpr(val), nil
	}
	return e, nil
}

// ValidateCols make sure that the cols in the expression match the tabledef
func (e *OpExpr) ValidateCols(profile *sqprofile.SQProfile, tab *TableDef) error {
	err := e.exL.ValidateCols(profile, tab)
	if err != nil {
		return err
	}
	err = e.exR.ValidateCols(profile, tab)
	return err
}

// NewOpExpr creates a new OpExpr and returns it as an Expr
func NewOpExpr(exL Expr, op string, exR Expr) Expr {
	return &OpExpr{exL: exL, Operator: op, exR: exR, name: "(" + exL.GetName() + op + exR.GetName() + ")"}
}

// Encode returns a binary encoded version of the expression
func (e *OpExpr) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)

	// Identify the type of Expression
	enc.Writebyte(IDOpExpr)
	enc.WriteString(e.name)

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
	e.name = dec.ReadString()
	e.Operator = dec.ReadString()
	e.exL = DecodeExpr(dec)

	e.exR = DecodeExpr(dec)

}

///////////////////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////////////////

// CountExpr stores information about a function to allow Evaluate() to determine the correct Value
type CountExpr struct {
	Cmd  string
	name string
	cnt  int
}

// GetLeft - CountExpr is a leaf node, it will always return nil
func (e *CountExpr) GetLeft() Expr {
	return nil
}

// GetRight - CountExpr is a leaf node, it will always return nil
func (e *CountExpr) GetRight() Expr {
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
	return "count()"
}

// GetName returns the name of the expression
func (e *CountExpr) GetName() string {
	return e.name
}

// GetColDef returns a column definition for the expression
func (e *CountExpr) GetColDef() ColDef {
	return ColDef{ColName: e.GetName(), ColType: "INT"}
}

// Evaluate -
func (e *CountExpr) Evaluate(profile *sqprofile.SQProfile, row *RowDef) (sqtypes.Value, error) {

	e.cnt++
	return nil, nil
}

// Reduce will colapse the expression to it's simplest form
func (e *CountExpr) Reduce() (Expr, error) {
	return e, nil
}

// ValidateCols make sure that the cols in the expression match the tabledef
func (e *CountExpr) ValidateCols(profile *sqprofile.SQProfile, tab *TableDef) error {
	return nil
}

// NewCountExpr creates a new CountExpr object
func NewCountExpr() Expr {
	return &CountExpr{name: "count()"}

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

///////////////////////////////////////////////////////////////////////////////////////////////////

// NegateExpr allows for an operator to create a value based on two other values
type NegateExpr struct {
	exL  Expr
	name string
}

// GetLeft - returns the left expression for NegateExpr
func (e *NegateExpr) GetLeft() Expr {
	return e.exL
}

// GetRight -returns the right expression for NegateExpr
func (e *NegateExpr) GetRight() Expr {
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
	return "(-" + e.exL.ToString() + ")"
}

// GetName returns the name of the expression
func (e *NegateExpr) GetName() string {
	return e.name
}

// GetColDef returns a column definition for the expression
func (e *NegateExpr) GetColDef() ColDef {
	col := e.exL.GetColDef()
	return ColDef{ColName: e.GetName(), ColType: col.ColType}
}

// Evaluate -
func (e *NegateExpr) Evaluate(profile *sqprofile.SQProfile, row *RowDef) (sqtypes.Value, error) {
	var retVal sqtypes.Value

	vL, err := e.exL.Evaluate(profile, row)
	if err != nil {
		return nil, err
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
func (e *NegateExpr) ValidateCols(profile *sqprofile.SQProfile, tab *TableDef) error {
	err := e.exL.ValidateCols(profile, tab)

	return err
}

// NewNegateExpr creates a new NegateExpr and returns it as an Expr
func NewNegateExpr(exL Expr) Expr {
	return &NegateExpr{exL: exL, name: "(-" + exL.GetName() + ")"}
}

// Encode returns a binary encoded version of the expression
func (e *NegateExpr) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	// Identify the type of Expression
	enc.Writebyte(IDNegateExpr)
	enc.WriteString(e.name)

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
	e.name = dec.ReadString()
	e.exL = DecodeExpr(dec)

}

///////////////////////////////////////////////////////////////////////////////////////////////////

// FuncExpr stores information about a function to allow Evaluate() to determine the correct Value
type FuncExpr struct {
	Cmd  string
	exL  Expr
	name string
}

// GetLeft - FuncExpr is a leaf node, it will always return nil
func (e *FuncExpr) GetLeft() Expr {
	return e.exL
}

// GetRight - FuncExpr is a leaf node, it will always return nil
func (e *FuncExpr) GetRight() Expr {
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
	return e.name
}

// GetName returns the name of the expression
func (e *FuncExpr) GetName() string {
	return e.name
}

// GetColDef returns a column definition for the expression
func (e *FuncExpr) GetColDef() ColDef {
	return ColDef{ColName: e.GetName(), ColType: "INT"}
}

// Evaluate takes the current Expression and calculates the results based on the given row
func (e *FuncExpr) Evaluate(profile *sqprofile.SQProfile, row *RowDef) (retVal sqtypes.Value, err error) {
	var vL sqtypes.Value

	vL, err = e.exL.Evaluate(profile, row)
	if err != nil {
		return
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
func (e *FuncExpr) ValidateCols(profile *sqprofile.SQProfile, tab *TableDef) error {
	return e.exL.ValidateCols(profile, tab)
}

// NewFuncExpr creates a new CountExpr object
func NewFuncExpr(cmd string, lExp Expr) Expr {
	return &FuncExpr{Cmd: cmd, exL: lExp, name: cmd + "(" + lExp.GetName() + ")"}

}

// Encode returns a binary encoded version of the expression
func (e *FuncExpr) Encode() *sqbin.Codec {
	//enc := sqbin.NewCodec(nil)
	panic("FuncExpr Encode not implemented")
	//return enc
}

// Decode gets a binary encoded version of the expression
func (e *FuncExpr) Decode(*sqbin.Codec) {
	panic("FuncExpr Decode not implemented")
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
