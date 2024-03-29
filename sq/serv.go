package sq

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqmutex"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqprotocol"
	"github.com/wilphi/sqsrv/sqtables"
	"github.com/wilphi/sqsrv/tokens"
)

var dispatcher = []struct {
	Exec   func(trans sqtables.Transaction, tkns *tokens.TokenList) (string, *sqtables.DataSet, error)
	First  tokens.TokenID
	Second tokens.TokenID
}{
	{Exec: cmd.Select, First: tokens.Select, Second: tokens.NilToken},
	{Exec: cmd.InsertInto, First: tokens.Insert, Second: tokens.Into},
	{Exec: cmd.Delete, First: tokens.Delete, Second: tokens.NilToken},
	{Exec: cmd.CreateTable, First: tokens.Create, Second: tokens.Table},
	{Exec: cmd.DropTable, First: tokens.Drop, Second: tokens.Table},
	{Exec: cmd.Update, First: tokens.Update, Second: tokens.NilToken},
}

// ShutdownType -
type ShutdownType byte

// Values for ShutdownType
const (
	NoAction      = 0
	Shutdown      = 1
	ShutdownForce = 2
)

// SrvCmds -
type SrvCmds struct {
	Exec    func(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error)
	First   string
	Second  string
	HelpTxt string
}

var srvCmds []SrvCmds

// Monitor -
type Monitor struct {
	Alloc,
	TotalAlloc,
	Sys,
	Mallocs,
	Frees,
	LiveObjects,
	PauseTotalNs uint64
	NumGC        uint32
	NumGoroutine int
}

func init() {
	srvCmds = []SrvCmds{
		{Exec: cmdShutdownForce, First: "shutdown", Second: "force", HelpTxt: "Immediately causes the termination of the sqsrv process.\n\t\t Inprocess requests will be abandoned."},
		{Exec: cmdShutdown, First: "shutdown", Second: "", HelpTxt: "Initiates an orderly termination of the sqsrv process.\n\t\t" +
			"It will stop accepting new requests and finish currently running request.\n\t\t" +
			"Finally a checkpoint will also be run."},
		{Exec: cmdStatsMem, First: "stats", Second: "mem", HelpTxt: "Displays statistics on memory usage and garbage \n\t\tcollection for the server"},
		{Exec: cmdStatsLock, First: "stats", Second: "lock", HelpTxt: "Displays statistics on lock usage and delays due to \n\t\tlocking for the server"},
		{Exec: cmdHelp, First: "stats", Second: "help", HelpTxt: "show various statistics about the server"},
		{Exec: cmdHelper, First: "stats", Second: "", HelpTxt: ""},
		{Exec: cmdGC, First: "gc", Second: "", HelpTxt: "Initiates a garbage collection on the server"},
		{Exec: cmdLock, First: "lock", Second: "", HelpTxt: "Sets a Write lock on the given table"},
		{Exec: cmdUnLock, First: "unlock", Second: "", HelpTxt: "Removes Write lock on the given table"},
		{Exec: help, First: "help", Second: "", HelpTxt: "\nSQSRV is an in-memory SQL server with persistance to disk.\n   SQL commands are a subset of SQL-92 and must be entered as a \n   single line of text. Valid SQL commands include:\n%s\n   Other commands are:"},
		{Exec: cmdShowTables, First: "show", Second: "tables", HelpTxt: "Displays the list of tables in the database"},
		{Exec: cmdShowTable, First: "show", Second: "table", HelpTxt: "Displays the structure of a given table"},
		{Exec: cmdShowConns, First: "show", Second: "conn", HelpTxt: "Displays the list of connections to the server"},
		{Exec: cmdHelp, First: "show", Second: "help", HelpTxt: "Displays information about the structure of the database"},
		{Exec: cmdHelper, First: "show", Second: "", HelpTxt: ""},
		{Exec: cmdCheckpoint, First: "checkpoint", Second: "", HelpTxt: "Ensures that all current data is durably written to disk"},
	}
}

// GetCmdFunc -
func GetCmdFunc(tkns tokens.TokenList) func(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	var firstVal, secondVal string

	if tkns.Len() <= 0 {
		return nil
	}
	vtkn, ok := tkns.Peek().(*tokens.ValueToken)
	if ok {
		firstVal = strings.ToLower(vtkn.Value())
	} else {
		firstVal = strings.ToLower(tkns.Peek().Name())
	}

	if tkns.Len() > 1 {
		vtkn2, ok := tkns.Peekx(1).(*tokens.ValueToken)
		if ok {
			secondVal = strings.ToLower(vtkn2.Value())
		} else {
			secondVal = strings.ToLower(tkns.Peekx(1).Name())
		}
	} else {
		secondVal = ""
	}
	for _, cmd := range srvCmds {

		if strings.ToLower(cmd.First) == firstVal {
			if tkns.Len() > 1 && strings.ToLower(cmd.Second) == secondVal {
				return cmd.Exec
			}
			if cmd.Second == "" {
				if cmd.Exec != nil {
					return cmd.Exec
				}

			}

		}
	}
	return nil
}

// GetDispatchFunc -
func GetDispatchFunc(tkns tokens.TokenList) func(trans sqtables.Transaction, tkns *tokens.TokenList) (string, *sqtables.DataSet, error) {
	if tkns.Len() <= 0 {
		return nil
	}
	for _, dis := range dispatcher {
		if dis.First == tkns.Peek().ID() {

			if tkns.Len() > 1 && dis.Second != tokens.NilToken {
				if dis.Second == tkns.Peekx(1).ID() {
					return dis.Exec
				}

			} else {
				return dis.Exec
			}
		}
	}
	return nil
}

// MemStats generates a string that contains current information of the status of the go environment Memory
func MemStats() string {
	var rtm runtime.MemStats

	// Read full mem stats
	runtime.ReadMemStats(&rtm)
	rawdata := [][]string{
		{"Allocated Heap", format(rtm.Alloc), "Bytes"},
		{"Total Allocations", format(rtm.TotalAlloc), "Bytes"},
		{"Memory from System", format(rtm.Sys), "Bytes"},
		{"Objects Allocated", format(rtm.Mallocs), ""},
		{"Objects Freed", format(rtm.Frees), ""},
		{"Live Objects", format(rtm.Mallocs - rtm.Frees), ""},
		{"Number of GC runs", format(uint64(rtm.NumGC)), ""},
		{"Number of GC pauses", format(rtm.PauseTotalNs), ""},
		{"Number of GoRoutines", format(uint64(runtime.NumGoroutine())), ""},
	}

	str := "Memory Stats:\n"
	for _, line := range rawdata {
		str += fmt.Sprintf("\t%-20s = %21s %s\n", line[0], line[1], line[2])
	}

	return str
}
func format(q uint64) string {
	lookup := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	str := ""
	i := 0
	for {
		r := int64(q % 10)
		q = q / 10
		if str != "" {
			if i%3 == 0 {
				str = "," + str
			}
			str = lookup[r] + str
		} else {
			str = lookup[r]
		}
		if q <= 0 {
			break
		}
		i++
	}
	return str
}

func cmdShutdown(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	resp := sqprotocol.ResponseToClient{Msg: "Server is shutting down...", IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	return resp, Shutdown, nil
}
func cmdShutdownForce(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	resp := sqprotocol.ResponseToClient{Msg: "Server is shutting down...", IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	return resp, ShutdownForce, nil
}
func cmdStatsMem(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	resp := sqprotocol.ResponseToClient{Msg: MemStats(), IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	return resp, NoAction, nil
}

func cmdStatsLock(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	resp := sqprotocol.ResponseToClient{Msg: sqmutex.GetMtxStats(), IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	return resp, NoAction, nil
}

func cmdGC(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	resp := sqprotocol.ResponseToClient{Msg: "", IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	start := time.Now()
	runtime.GC()
	elapsed := time.Since(start)
	tMsg := fmt.Sprintf("Garbage collection completed in %s", elapsed.Round(time.Millisecond))
	fmt.Println(tMsg)
	resp.Msg = tMsg

	return resp, NoAction, nil
}
func cmdLock(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	resp := sqprotocol.ResponseToClient{Msg: "", IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	tkns.Remove()
	if tkns.IsA(tokens.Ident) {
		tkn := tkns.Peek()
		tableName := tkn.(*tokens.ValueToken).Value()
		td, err := sqtables.GetTable(profile, tableName)
		if td == nil || err != nil {
			resp.IsErr = true
			resp.Msg = "Table not found"
			if err != nil {
				resp.Msg = err.Error()
			}
		} else {
			resp.Msg = "Locking table " + td.GetName(profile)
			err := td.Lock(profile)
			if err != nil {
				resp.IsErr = true
				resp.Msg = err.Error()
			}
		}

	} else {
		resp.IsErr = true
		resp.Msg = "Lock command must be followed by tablename"
	}
	return resp, NoAction, nil
}

func cmdUnLock(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	resp := sqprotocol.ResponseToClient{Msg: "", IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	tkns.Remove()
	if tkns.IsA(tokens.Ident) {
		tkn := tkns.Peek()
		tableName := tkn.(*tokens.ValueToken).Value()
		td, err := sqtables.GetTable(profile, tableName)
		if td == nil || err != nil {
			resp.IsErr = true
			resp.Msg = "Table not found"
			if err != nil {
				resp.Msg = err.Error()
			}
		} else {
			td.Unlock(profile)
			resp.Msg = "Unlocked table " + td.GetName(profile)

		}

	} else {
		resp.IsErr = true
		resp.Msg = "Unlock command must be followed by tablename"
	}
	return resp, NoAction, nil
}

func cmdShowTables(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	tables, rows, err := sqtables.CatalogTablesWithCount(profile)
	if err != nil {
		resp := sqprotocol.ResponseToClient{Msg: err.Error(), IsErr: true, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
		return resp, NoAction, nil
	}
	str := "Table List\n--------------------------------------------\n"
	for i, tab := range tables {
		str += fmt.Sprintf("  %-20s %20s\n", tab, format(rows[i]))
	}
	resp := sqprotocol.ResponseToClient{Msg: str, IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	return resp, NoAction, nil
}

func cmdShowTable(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	resp := sqprotocol.ResponseToClient{Msg: "", IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	tkns.Remove()
	tkns.Remove()
	if tkns.IsA(tokens.Ident) {
		tkn := tkns.Peek()
		tableName := tkn.(*tokens.ValueToken).Value()
		td, err := sqtables.GetTable(profile, tableName)
		if td == nil || err != nil {
			resp.IsErr = true
			resp.Msg = "Table \"" + tableName + "\" not found"
			if err != nil {
				resp.Msg = err.Error()
			}
		} else {
			resp.Msg = td.String(profile)
		}

	} else {
		resp.IsErr = true
		resp.Msg = "show table command must be followed by tablename"
	}
	return resp, NoAction, nil
}

func sqlHelper() string {
	var lines string

	for _, cmd := range dispatcher {
		if cmd.Second == tokens.NilToken {
			lines += "\t" + tokens.IDName(cmd.First)
		} else {
			lines += "\t" + tokens.IDName(cmd.First) + " " + tokens.IDName(cmd.Second)
		}
	}
	return lines
}
func cmdHelper(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	vtkn, ok := tkns.Peek().(*tokens.ValueToken)
	cmdtxt := sqtables.Ternary(ok, vtkn.Value(), "")
	resp := sqprotocol.ResponseToClient{
		Msg:         fmt.Sprintf("Invalid %s command, try %s help for more information", cmdtxt, cmdtxt),
		IsErr:       false,
		HasData:     false,
		NRows:       0,
		NCols:       0,
		CMDResponse: true,
	}
	return resp, NoAction, nil
}

// cmdHelp generates the help text for a command
func cmdHelp(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	vtkn, ok := tkns.Peek().(*tokens.ValueToken)
	cmdtxt := sqtables.Ternary(ok, strings.ToLower(vtkn.Value()), "")
	var firstline, bodytxt string
	for _, cmd := range srvCmds {
		if cmd.First == cmdtxt {
			if cmd.Second != "" {
				if cmd.Second == "help" {
					firstline = fmt.Sprintf("%s: %s\n", cmd.First, cmd.HelpTxt)
				} else {
					bodytxt += fmt.Sprintf("\t%s - %s\n", cmd.Second, cmd.HelpTxt)
				}

			}
		}
	}

	resp := sqprotocol.ResponseToClient{
		Msg:         firstline + bodytxt,
		IsErr:       false,
		HasData:     false,
		NRows:       0,
		NCols:       0,
		CMDResponse: true,
	}
	return resp, NoAction, nil
}

// help generates the help text for all server commands
func help(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	lines := []string{"To Be Replaced"}
	//var firstline, bodytxt string

	for _, cmd := range srvCmds {
		if cmd.First == "help" {
			lines[0] = fmt.Sprintf(cmd.HelpTxt, sqlHelper())
		} else {
			if cmd.Second != "help" && cmd.HelpTxt != "" {
				if cmd.Second == "" {
					lines = append(lines, fmt.Sprintf("\t%s - %s", cmd.First, cmd.HelpTxt))
				} else {
					lines = append(lines, fmt.Sprintf("\t%s %s - %s", cmd.First, cmd.Second, cmd.HelpTxt))
				}

			}
		}
	}

	resp := sqprotocol.ResponseToClient{
		Msg:         strings.Join(lines, "\n"),
		IsErr:       false,
		HasData:     false,
		NRows:       0,
		NCols:       0,
		CMDResponse: true,
	}
	return resp, NoAction, nil
}

func cmdShowConns(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {

	resp := sqprotocol.ResponseToClient{
		Msg:         sqprotocol.ShowConn(),
		IsErr:       false,
		HasData:     false,
		NRows:       0,
		NCols:       0,
		CMDResponse: true,
	}
	return resp, NoAction, nil
}

func cmdCheckpoint(profile *sqprofile.SQProfile, tkns *tokens.TokenList) (sqprotocol.ResponseToClient, ShutdownType, error) {
	resp := sqprotocol.ResponseToClient{
		Msg:         "",
		IsErr:       false,
		HasData:     false,
		NRows:       0,
		NCols:       0,
		CMDResponse: true,
	}
	err := sqtables.WriteDB(profile)
	if err != nil {
		resp.Msg = "Error Writting to database: " + err.Error()
		resp.IsErr = true
	} else {
		resp.Msg = "Checkpoint Successful"
	}

	return resp, NoAction, err
}
