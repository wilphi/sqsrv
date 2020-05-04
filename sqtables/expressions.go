package sqtables

import (
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/wilphi/sqsrv/sqbin"
	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqtypes"
	"github.com/wilphi/sqsrv/tokens"
)

// Expr constants for binary versions
const (
	TMValueExpr = iota + 48
	TMColExpr
	TMOpExpr
	TMAggregateFunExpr
	TMNegateExpr
)

func init() {
	sqbin.RegisterType("TMValueExpr", TMValueExpr)
	sqbin.RegisterType("TMColExpr", TMColExpr)
	sqbin.RegisterType("TMOpExpr", TMOpExpr)
	sqbin.RegisterType("TMAggregateFunExpr", TMAggregateFunExpr)
	sqbin.RegisterType("TMNegateExpr", TMNegateExpr)
}

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
	String() string
	Build(b *strings.Builder)
	Name() string
	ColDef() ColDef
	ColDefs(tables ...*TableDef) []ColDef
	Evaluate(profile *sqprofile.SQProfile, partial bool, rows ...RowInterface) (sqtypes.Value, error)
	Reduce() (Expr, error)
	ValidateCols(profile *sqprofile.SQProfile, tables *TableList) error
	Encode() *sqbin.Codec
	Decode(*sqbin.Codec)
	SetAlias(alias string)
	IsAggregate() bool
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

// String - string representation of Expression. Will traverse to child conditions to form full string
func (e *ValueExpr) String() string {
	var b strings.Builder

	e.Build(&b)

	return b.String()
}

// Build - uses a Builder to create a string representation of the Expression
func (e *ValueExpr) Build(b *strings.Builder) {
	b.WriteString(e.v.String())
	if e.alias != "" {
		b.WriteString(" ")
		b.WriteString(e.alias)
	}
}

// Name returns the name of the expression
func (e *ValueExpr) Name() string {
	if e.alias != "" {
		return e.alias
	}
	return e.String()
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
func (e *ValueExpr) Evaluate(profile *sqprofile.SQProfile, partial bool, rows ...RowInterface) (sqtypes.Value, error) {
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
	enc.WriteTypeMarker(TMValueExpr)

	enc.WriteString(e.alias)
	// Write out the value
	e.v.Write(enc)

	return enc
}

// Decode gets a binary encoded version of the expression
func (e *ValueExpr) Decode(dec *sqbin.Codec) {
	dec.ReadTypeMarker(TMValueExpr)
	e.alias = dec.ReadString()
	e.v = sqtypes.ReadValue(dec)

}

//SetAlias sets an alternative name for the expression
func (e *ValueExpr) SetAlias(alias string) {
	e.alias = alias
}

// IsAggregate is true if the expression contains an aggregate function
func (e *ValueExpr) IsAggregate() bool {
	return false
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

// String - string representation of Expression. Will traverse to child conditions to form full string
func (e *ColExpr) String() string {
	var b strings.Builder

	e.Build(&b)

	return b.String()
}

// Build - uses a Builder to create a string representation of the Expression
func (e *ColExpr) Build(b *strings.Builder) {
	if e.alias == e.col.ColName {
		b.WriteString(e.alias)
		return
	}

	b.WriteString(e.col.DisplayName())
	if e.alias != "" {
		b.WriteString(" ")
		b.WriteString(e.alias)
	}
}

// Name returns the name of the expression
func (e *ColExpr) Name() string {
	if e.alias != "" {
		return e.alias
	}
	return e.String()
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
func (e *ColExpr) Evaluate(profile *sqprofile.SQProfile, partial bool, rows ...RowInterface) (sqtypes.Value, error) {
	var row RowInterface

	// Find the row with the proper table name
	for _, rw := range rows {
		if e.col.TableName == rw.GetTableName(profile) {
			row = rw
			break
		}
	}
	if row == nil {
		// no table found
		if !partial {
			str := ""
			for _, rw := range rows {
				str += rw.GetTableName(profile) + ", "
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
	if e.col.TableName == DataSetTableName {
		return nil
	}

	cd, err := tables.FindColDef(profile, e.col.ColName, e.col.TableName)
	if err != nil {
		return err
	}
	e.col, err = MergeColDef(e.col, *cd)
	return err
}

// NewColExpr creates a new ColExpr object
func NewColExpr(c ColDef) Expr {
	return &ColExpr{col: c}

}

// Encode returns a binary encoded version of the expression
func (e *ColExpr) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)
	enc.WriteTypeMarker(TMColExpr)
	enc.WriteString(e.alias)

	// Write out the value
	e.col.Encode(enc)

	return enc
}

// Decode gets a binary encoded version of the expression
func (e *ColExpr) Decode(dec *sqbin.Codec) {
	dec.ReadTypeMarker(TMColExpr)

	e.alias = dec.ReadString()
	e.col.Decode(dec)

}

//SetAlias sets an alternative name for the expression
func (e *ColExpr) SetAlias(alias string) {
	e.alias = alias
}

// IsAggregate is true if the expression contains an aggregate function
func (e *ColExpr) IsAggregate() bool {
	return false
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// OpExpr allows for an operator to create a value based on two other values
type OpExpr struct {
	exL, exR Expr
	Operator tokens.TokenID
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

// String - string representation of Expression. Will traverse to child conditions to form full string
func (e *OpExpr) String() string {
	var b strings.Builder

	e.Build(&b)
	return b.String()
}

// Build - uses a Builder to create a string representation of the Expression
func (e *OpExpr) Build(b *strings.Builder) {
	b.WriteString("(")
	e.exL.Build(b)
	b.WriteString(tokens.IDName(e.Operator))
	e.exR.Build(b)
	b.WriteString(")")

	if e.alias != "" {
		b.WriteString(" ")
		b.WriteString(e.alias)
	}
}

// Name returns the name of the expression
func (e *OpExpr) Name() string {
	if e.alias != "" {
		return e.alias
	}
	return e.String()
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
func (e *OpExpr) Evaluate(profile *sqprofile.SQProfile, partial bool, rows ...RowInterface) (sqtypes.Value, error) {

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
func NewOpExpr(exL Expr, op tokens.TokenID, exR Expr) Expr {
	return &OpExpr{exL: exL, Operator: op, exR: exR}
}

// Encode returns a binary encoded version of the expression
func (e *OpExpr) Encode() *sqbin.Codec {
	enc := sqbin.NewCodec(nil)

	// Identify the type of Expression
	enc.WriteTypeMarker(TMOpExpr)
	enc.WriteString(e.alias)

	enc.WriteUint64(uint64(e.Operator))

	tmp := e.exL.Encode()
	enc.Write(tmp.Bytes())

	tmp = e.exR.Encode()
	enc.Write(tmp.Bytes())

	return enc
}

// Decode gets a binary encoded version of the expression
func (e *OpExpr) Decode(dec *sqbin.Codec) {
	dec.ReadTypeMarker(TMOpExpr)

	e.alias = dec.ReadString()
	e.Operator = tokens.TokenID(dec.ReadUint64())
	e.exL = DecodeExpr(dec)

	e.exR = DecodeExpr(dec)

}

//SetAlias sets an alternative name for the expression
func (e *OpExpr) SetAlias(alias string) {
	e.alias = alias
}

// IsAggregate is true if the expression contains an aggregate function
func (e *OpExpr) IsAggregate() bool {
	return e.exL.IsAggregate() || e.exR.IsAggregate()
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

// String - string representation of Expression. Will traverse to child conditions to form full string
func (e *NegateExpr) String() string {
	var b strings.Builder

	e.Build(&b)
	return b.String()
}

// Build - uses a Builder to create a string representation of the Expression
func (e *NegateExpr) Build(b *strings.Builder) {
	b.WriteString("(-")
	e.exL.Build(b)
	b.WriteString(")")

	if e.alias != "" {
		b.WriteString(" ")
		b.WriteString(e.alias)
	}
}

// Name returns the name of the expression
func (e *NegateExpr) Name() string {
	if e.alias != "" {
		return e.alias
	}
	return e.String()
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
func (e *NegateExpr) Evaluate(profile *sqprofile.SQProfile, partial bool, rows ...RowInterface) (sqtypes.Value, error) {

	vL, err := e.exL.Evaluate(profile, partial, rows...)
	if err != nil {
		return nil, err
	}
	if vL == nil {
		return nil, nil
		//return nil, sqerr.Newf("Unable to evaluate %q", e.exL.Name())
	}

	n, ok := vL.(sqtypes.Negatable)
	if !ok {
		return vL, sqerr.NewSyntaxf("%s values can not be negated", tokens.IDName(vL.Type()))
	}

	return n.Negate(), nil
}

// Reduce will colapse the expression to it's simplest form
func (e *NegateExpr) Reduce() (Expr, error) {

	eL, err := e.exL.Reduce()
	if err != nil {
		return e, err
	}
	e.SetLeft(eL)
	vL, okL := eL.(*ValueExpr)

	if okL {
		val := vL.v
		n, ok := val.(sqtypes.Negatable)
		if !ok {
			return vL, sqerr.NewSyntaxf("%s values can not be negated", tokens.IDName(val.Type()))
		}

		return NewValueExpr(n.Negate()), nil
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
	enc.WriteTypeMarker(TMNegateExpr)
	enc.WriteString(e.alias)

	tmp := e.exL.Encode()
	enc.Write(tmp.Bytes())

	return enc
}

// Decode gets a binary encoded version of the expression
func (e *NegateExpr) Decode(dec *sqbin.Codec) {
	dec.ReadTypeMarker(TMNegateExpr)

	e.alias = dec.ReadString()
	e.exL = DecodeExpr(dec)

}

//SetAlias sets an alternative name for the expression
func (e *NegateExpr) SetAlias(alias string) {
	e.alias = alias
}

// IsAggregate is true if the expression contains an aggregate function
func (e *NegateExpr) IsAggregate() bool {
	return e.exL.IsAggregate()
}

///////////////////////////////////////////////////////////////////////////////////////////////////

// FuncExpr stores information about a function to allow Evaluate() to determine the correct Value
type FuncExpr struct {
	Cmd   tokens.TokenID
	exL   Expr
	alias string
}

// Left - FuncExpr may have an expression
func (e *FuncExpr) Left() Expr {
	return e.exL
}

// Right - FuncExpr currently implements functions with 0 or 1 arguments
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

// String - string representation of Expression. Will traverse to child conditions to form full string
func (e *FuncExpr) String() string {
	var b strings.Builder

	e.Build(&b)
	return b.String()
}

// Build - uses a Builder to create a string representation of the Expression
func (e *FuncExpr) Build(b *strings.Builder) {
	b.WriteString(tokens.IDName(e.Cmd))
	b.WriteString("(")
	if e.exL != nil {
		e.exL.Build(b)
	}
	b.WriteString(")")

	if e.alias != "" {
		b.WriteString(" ")
		b.WriteString(e.alias)
	}
}

// Name returns the name of the expression
func (e *FuncExpr) Name() string {
	if e.alias != "" {
		return e.alias
	}
	return e.String()
}

// ColDef returns a column definition for the expression
func (e *FuncExpr) ColDef() ColDef {
	name := e.Name()
	//???? INT for Count, FUNC otherwise????????
	colType := e.Cmd

	return ColDef{ColName: name, ColType: colType}
}

// ColDefs returns a list of all actual columns in the expression
func (e *FuncExpr) ColDefs(tables ...*TableDef) []ColDef {
	if e.exL != nil {
		colsL := e.exL.ColDefs(tables...)
		return colsL
	}
	return nil
}

// Evaluate takes the current Expression and calculates the results based on the given row
func (e *FuncExpr) Evaluate(profile *sqprofile.SQProfile, partial bool, rows ...RowInterface) (retVal sqtypes.Value, err error) {
	var vL sqtypes.Value

	if e.exL == nil {
		if e.Cmd == tokens.Count {
			return sqtypes.NewSQNull(), nil
		}
		return nil, nil
	}
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

func evalFunc(cmd tokens.TokenID, v sqtypes.Value) (retVal sqtypes.Value, err error) {

	switch cmd {
	case tokens.Float, tokens.Int, tokens.Bool, tokens.String:
		retVal, err = v.Convert(cmd)
	case tokens.Count, tokens.Sum, tokens.Avg, tokens.Min, tokens.Max:
		// aggregate functions are evaluated elsewhere, just pass the data along
		retVal = v
	default:
		err = sqerr.NewSyntaxf("%q is not a valid function", tokens.IDName(cmd))
	}
	return
}

// IsAggregate returns true if the function is an aggregate
func (e *FuncExpr) IsAggregate() bool {
	tkn := tokens.GetWordToken(e.Cmd)
	ret := tkn.TestFlags(tokens.IsAggregate)
	if !ret && e.exL != nil {
		return e.exL.IsAggregate()
	}
	return ret
}

// Reduce will colapse the expression to it's simplest form
func (e *FuncExpr) Reduce() (Expr, error) {
	if e.exL == nil {
		return e, nil
	}
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
	if e.exL != nil {
		return e.exL.ValidateCols(profile, tables)
	}
	return nil
}

// NewFuncExpr creates a new CountExpr object
func NewFuncExpr(cmd tokens.TokenID, lExp Expr) Expr {
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

	tm := dec.PeekTypeMarker()
	switch tm {
	case TMValueExpr:
		ex = &ValueExpr{}
	case TMColExpr:
		ex = &ColExpr{}
	case TMOpExpr:
		ex = &OpExpr{}
	case TMNegateExpr:
		ex = &NegateExpr{}
	case TMAggregateFunExpr:
		log.Panic("Unexpected Count expression in Decode")
	default:
		log.Panicf("Unexpected expression type in Decode: %d-%s", tm, sqbin.TMToString(tm))

	}
	ex.Decode(dec)
	return ex
}

//FindAggregateFuncs finds the aggregate functions in the expression
func FindAggregateFuncs(e Expr) []FuncExpr {
	var flist []FuncExpr
	var elist []Expr

	elist = append(elist, e)

	for len(elist) > 0 {
		exp := elist[0]
		elist = elist[1:]
		if exp.IsAggregate() {
			fexpr, ok := exp.(*FuncExpr)
			if ok && tokens.GetWordToken(fexpr.Cmd).TestFlags(tokens.IsAggregate) {
				flist = append(flist, *fexpr)
			}
			el := exp.Left()
			er := exp.Right()
			if el != nil {
				elist = append(elist, el)
			}
			if er != nil {
				elist = append(elist, er)
			}
		}

	}
	return flist

}

//ProcessHaving reworks the having clause
func ProcessHaving(e Expr, flist []FuncExpr, cnt int) (Expr, []FuncExpr, int) {
	var alias string
	var exp, lexpr, rexpr Expr

	if e.IsAggregate() {
		fexpr, ok := e.(*FuncExpr)
		if ok && tokens.GetWordToken(fexpr.Cmd).TestFlags(tokens.IsAggregate) {
			alias = " Hidden_" + fexpr.Name()
			fexpr.SetAlias(alias)
			flist = append(flist, *fexpr)
			exp = NewColExpr(ColDef{ColName: alias, Idx: cnt, TableName: DataSetTableName})
			cnt++
			return exp, flist, cnt
		}
		exp = e
		el := e.Left()
		er := e.Right()
		if el != nil {
			lexpr, flist, cnt = ProcessHaving(el, flist, cnt)
			exp.SetLeft(lexpr)
		}
		if er != nil {
			rexpr, flist, cnt = ProcessHaving(er, flist, cnt)
			exp.SetRight(rexpr)

		}
	} else {
		exp = e
	}

	return exp, flist, cnt

}
