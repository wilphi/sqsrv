package sqtables

import (
	"fmt"

	"github.com/wilphi/sqsrv/sqprofile"
	t "github.com/wilphi/sqsrv/tokens"

	log "github.com/sirupsen/logrus"

	"github.com/wilphi/sqsrv/sqerr"
	"github.com/wilphi/sqsrv/sqtypes"
)

// Condition - base condition type
type Condition interface {
	GetLeft() Condition
	GetRight() Condition
	//	SetLeft(cond Condition)
	//	SetRight(cond Condition)
	ToString() string
	Evaluate(profile *sqprofile.SQProfile, row *RowDef) (bool, error)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
//
// CompareValCond
//
///////////////////////////////////////////////////////////////////////////////////////////////////

// CompareValCond -
type CompareValCond struct {
	Col      ColDef
	Operator string
	Val      sqtypes.Value
}

// GetLeft - CompareValCond is a leaf node, it will always return nil
func (cv *CompareValCond) GetLeft() Condition {
	return nil
}

// GetRight - CompareValCond is a leaf node, it will always return nil
func (cv *CompareValCond) GetRight() Condition {
	return nil
}

/*
// SetLeft -
func (cv *CompareValCond) SetLeft(cond Condition) {
	log.Panic("Invalid to SetLeft on a CompareValCond")
}

// SetRight -
func (cv *CompareValCond) SetRight(cond Condition) {
	log.Panic("Invalid to SetRight on a CompareValCond")

}
*/

// ToString - string representation of Condition. Will traverse to child conditions to form full string
func (cv *CompareValCond) ToString() string {
	fmtString := "%s %s %s"
	if cv.Val.GetType() == t.TypeString {
		fmtString = "%s %s %q"
	}
	return fmt.Sprintf(fmtString, cv.Col.ColName, cv.Operator, cv.Val.ToString())
}

// Evaluate -
func (cv *CompareValCond) Evaluate(profile *sqprofile.SQProfile, row *RowDef) (bool, error) {
	// Get row value
	rowValue, err := row.GetColData(profile, &cv.Col)
	if err != nil {
		log.Error(err)
		return false, err
	}

	if cv.Val.GetType() != rowValue.GetType() {
		err = sqerr.New(fmt.Sprintf("Type Mismatch in Where clause expression: %s(%s) %s %s(%s)", cv.Col.ColName, cv.Col.ColType, cv.Operator, cv.Val.ToString(), cv.Val.GetType()))
		log.Error(err)
		return false, err
	}
	ret := false
	switch cv.Operator {
	case "=":
		ret = rowValue.Equal(cv.Val)
	case "<":
		ret = rowValue.LessThan(cv.Val)
	case ">":
		ret = rowValue.GreaterThan(cv.Val)
	default:
		return false, sqerr.NewInternal(fmt.Sprintf("Operator %s is not implemented", cv.Operator))
	}
	return ret, nil
}

// NewCVCond -
func NewCVCond(col ColDef, op string, val sqtypes.Value) Condition {

	ret := &CompareValCond{Col: col, Operator: op, Val: val}

	return ret
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// LogicCond
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// LogicCond structure
type LogicCond struct {
	leftCond, rightCond Condition
	operator            string
}

// GetLeft -
func (lc *LogicCond) GetLeft() Condition {
	return lc.leftCond
}

// GetRight -
func (lc *LogicCond) GetRight() Condition {
	return lc.rightCond
}

/*
// SetLeft -
func (lc *LogicCond) SetLeft(cond Condition) {
	lc.leftCond = cond
}

// SetRight -
func (lc *LogicCond) SetRight(cond Condition) {
	lc.rightCond = cond
}
*/

// ToString -
func (lc *LogicCond) ToString() string {
	return "(" + lc.leftCond.ToString() + " " + lc.operator + " " + lc.rightCond.ToString() + ")"
}

// Evaluate - Evaluate the condition with given row. This function will
// recursively evaluate child conditions as well
func (lc *LogicCond) Evaluate(profile *sqprofile.SQProfile, row *RowDef) (bool, error) {

	var ret, leftV, rightV bool
	var err error

	leftV, err = lc.leftCond.Evaluate(profile, row)
	if err != nil {
		return false, err
	}

	switch lc.operator {
	case "AND":
		if !leftV {
			// Left value is already false, so ret will always be false no matter the right value
			ret = leftV
		} else {
			rightV, err = lc.rightCond.Evaluate(profile, row)
			if err != nil {
				return false, err
			}

			ret = leftV && rightV
		}
	case "OR":
		if leftV {
			// left value is true, so ret will always be true
			ret = leftV
		} else {
			rightV, err = lc.rightCond.Evaluate(profile, row)
			if err != nil {
				return false, err
			}

			ret = leftV || rightV
		}
	default:
		log.Fatal("Invalid logic Operator", lc.operator)
	}

	return ret, nil
}

// NewANDCondition - Create an AND condition
func NewANDCondition(left, right Condition) Condition {
	return &LogicCond{leftCond: left, rightCond: right, operator: "AND"}
}

// NewORCondition - Create a new OR condition
func NewORCondition(left, right Condition) Condition {
	return &LogicCond{leftCond: left, rightCond: right, operator: "OR"}
}
