package serv

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/wilphi/sqsrv/cmd"
	"github.com/wilphi/sqsrv/sqmutex"
	"github.com/wilphi/sqsrv/sqprofile"
	"github.com/wilphi/sqsrv/sqprotocol"
	"github.com/wilphi/sqsrv/sqprotocol/server"
	"github.com/wilphi/sqsrv/sqtables"
	tk "github.com/wilphi/sqsrv/tokens"
)

var dispatcher = []struct {
	Exec   func(profile *sqprofile.SQProfile, tkns *tk.TokenList) (string, *sqtables.DataSet, error)
	First  string
	Second string
}{
	{Exec: cmd.Select, First: tk.Select, Second: ""},
	{Exec: cmd.InsertInto, First: tk.Insert, Second: tk.Into},
	{Exec: cmd.Delete, First: tk.Delete, Second: ""},
	{Exec: cmd.CreateTable, First: tk.Create, Second: tk.Table},
}

// SrvCmds -
type SrvCmds struct {
	Exec    func(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error)
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
		{Exec: cmdShutdown, First: "shutdown", Second: "", HelpTxt: "Initiates an orderly termination of the sqsrv process."},
		{Exec: cmdStatsMem, First: "stats", Second: "mem", HelpTxt: "Displays statistics on memory usage and garbage \n\t\tcollection for the server"},
		{Exec: cmdStatsLock, First: "stats", Second: "lock", HelpTxt: "Displays statistics on lock usage and delays due to \n\t\tlocking for the server"},
		{Exec: cmdHelp, First: "stats", Second: "help", HelpTxt: "show various statistics about the server"},
		{Exec: cmdHelper, First: "stats", Second: "", HelpTxt: ""},
		{Exec: cmdGC, First: "gc", Second: "", HelpTxt: "Initiates a garbage collection on the server"},
		{Exec: cmdLock, First: "lock", Second: "", HelpTxt: "Sets a Write lock on the given table"},
		{Exec: cmdUnLock, First: "unlock", Second: "", HelpTxt: "Removes Write lock on the given table"},
		{Exec: help, First: "help", Second: "", HelpTxt: "\nSQSRV is an in-memory SQL server with persistance to disk.\n   SQL commands are a subset of SQL-92 and must be entered as a \n   single line of text. Valid SQL commands include:\n%s\n   Other commands are:"},
		{Exec: cmdShowTables, First: "show", Second: "tables", HelpTxt: "Displays the list of tables in the database"},
		{Exec: cmdShowConns, First: "show", Second: "conn", HelpTxt: "Displays the list of connections to the server"},
		{Exec: cmdHelp, First: "show", Second: "help", HelpTxt: "Displays information about the structure of the database"},
		{Exec: cmdHelper, First: "show", Second: "", HelpTxt: ""},
		{Exec: cmdCheckpoint, First: "checkpoint", Second: "", HelpTxt: "Ensures that all current data is durably written to disk"},
	}
}

// GetCmdFunc -
func GetCmdFunc(tkns tk.TokenList) func(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error) {
	if tkns.Len() <= 0 {
		return nil
	}
	for _, cmd := range srvCmds {
		if strings.ToLower(cmd.First) == strings.ToLower(tkns.Peek().GetValue()) {
			if tkns.Len() > 1 && strings.ToLower(cmd.Second) == strings.ToLower(tkns.Peekx(1).GetValue()) {
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
func GetDispatchFunc(tkns tk.TokenList) func(profile *sqprofile.SQProfile, tkns *tk.TokenList) (string, *sqtables.DataSet, error) {
	if tkns.Len() <= 0 {
		return nil
	}
	for _, dis := range dispatcher {
		if dis.First == tkns.Peek().GetName() {

			if tkns.Len() > 1 && dis.Second != "" {
				if dis.Second == tkns.Peekx(1).GetName() {
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
	var m Monitor
	var rtm runtime.MemStats

	// Read full mem stats
	runtime.ReadMemStats(&rtm)

	// Number of goroutines
	m.NumGoroutine = runtime.NumGoroutine()

	// Misc memory stats
	m.Alloc = rtm.Alloc
	m.TotalAlloc = rtm.TotalAlloc
	m.Sys = rtm.Sys
	m.Mallocs = rtm.Mallocs
	m.Frees = rtm.Frees

	// Live objects = Mallocs - Frees
	m.LiveObjects = m.Mallocs - m.Frees

	// GC Stats
	m.PauseTotalNs = rtm.PauseTotalNs
	m.NumGC = rtm.NumGC

	// Just encode to json and print
	b, _ := json.Marshal(m)
	str := string(b)
	fmt.Println(str)
	return str
}

// NewMonitor -
func NewMonitor(duration int) {
	var m Monitor
	var rtm runtime.MemStats
	var interval = time.Duration(duration) * time.Second
	for {
		<-time.After(interval)

		// Read full mem stats
		runtime.ReadMemStats(&rtm)

		// Number of goroutines
		m.NumGoroutine = runtime.NumGoroutine()

		// Misc memory stats
		m.Alloc = rtm.Alloc
		m.TotalAlloc = rtm.TotalAlloc
		m.Sys = rtm.Sys
		m.Mallocs = rtm.Mallocs
		m.Frees = rtm.Frees

		// Live objects = Mallocs - Frees
		m.LiveObjects = m.Mallocs - m.Frees

		// GC Stats
		m.PauseTotalNs = rtm.PauseTotalNs
		m.NumGC = rtm.NumGC

		// Just encode to json and print
		b, _ := json.Marshal(m)
		fmt.Println(string(b))
	}
}

func cmdShutdown(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error) {
	resp := sqprotocol.ResponseToClient{Msg: "Server is shutting down...", IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	return resp, true, nil
}

func cmdStatsMem(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error) {
	resp := sqprotocol.ResponseToClient{Msg: MemStats(), IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	return resp, false, nil
}

func cmdStatsLock(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error) {
	resp := sqprotocol.ResponseToClient{Msg: sqmutex.GetStats(), IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	return resp, false, nil
}

func cmdGC(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error) {
	resp := sqprotocol.ResponseToClient{Msg: "", IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	start := time.Now()
	runtime.GC()
	elapsed := time.Since(start)
	tMsg := fmt.Sprintf("Garbage collection completed in %s", elapsed.Round(time.Millisecond))
	fmt.Println(tMsg)
	resp.Msg = tMsg

	return resp, false, nil
}
func cmdLock(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error) {
	resp := sqprotocol.ResponseToClient{Msg: "", IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	tkns.Remove()
	if tkns.Test(tk.Ident) != "" {
		tableName := tkns.Peek().GetValue()
		td := sqtables.GetTable(profile, tableName)
		if td == nil {
			resp.IsErr = true
			resp.Msg = "Table not found"
		} else {
			resp.Msg = "Locking table " + td.GetName(profile)
			td.Lock(profile)
		}

	} else {
		resp.IsErr = true
		resp.Msg = "Lock command must be followed by tablename"
	}
	return resp, false, nil
}

func cmdUnLock(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error) {
	resp := sqprotocol.ResponseToClient{Msg: "", IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	tkns.Remove()
	if tkns.Test(tk.Ident) != "" {
		tableName := tkns.Peek().GetValue()
		td := sqtables.GetTable(profile, tableName)
		if td == nil {
			resp.IsErr = true
			resp.Msg = "Table not found"
		} else {
			td.Unlock(profile)
			resp.Msg = "Unlocked table " + td.GetName(profile)

		}

	} else {
		resp.IsErr = true
		resp.Msg = "Unlock command must be followed by tablename"
	}
	return resp, false, nil
}

func cmdShowTables(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error) {
	tables := sqtables.ListTables(profile)
	str := "Table List\n----------------------\n"
	for _, tab := range tables {
		str += fmt.Sprintf("  %-20s\n", tab)
	}
	resp := sqprotocol.ResponseToClient{Msg: str, IsErr: false, HasData: false, NRows: 0, NCols: 0, CMDResponse: true}
	return resp, false, nil
}

func sqlHelper() string {
	var lines string

	for _, cmd := range dispatcher {
		if cmd.Second == "" {
			lines += "\t" + cmd.First
		} else {
			lines += "\t" + cmd.First + " " + cmd.Second
		}
	}
	return lines
}
func cmdHelper(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error) {
	cmdtxt := tkns.Peek().GetValue()
	resp := sqprotocol.ResponseToClient{
		Msg:         fmt.Sprintf("Invalid %s command, try %s help for more information", cmdtxt, cmdtxt),
		IsErr:       false,
		HasData:     false,
		NRows:       0,
		NCols:       0,
		CMDResponse: true,
	}
	return resp, false, nil
}

// cmdHelp generates the help text for a command
func cmdHelp(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error) {
	cmdtxt := strings.ToLower(tkns.Peek().GetValue())
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
	return resp, false, nil
}

// help generates the help text for all server commands
func help(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error) {
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
	return resp, false, nil
}

func cmdShowConns(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error) {

	resp := sqprotocol.ResponseToClient{
		Msg:         server.ShowConn(),
		IsErr:       false,
		HasData:     false,
		NRows:       0,
		NCols:       0,
		CMDResponse: true,
	}
	return resp, false, nil
}

func cmdCheckpoint(profile *sqprofile.SQProfile, tkns *tk.TokenList) (sqprotocol.ResponseToClient, bool, error) {
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

	return resp, false, err
}
