# SQSRV #
## What is SQSRV ##
SQSRV is Small SQL SeRVer written in GO. It uses a subset of SQL and is an in-memory database server with persistance to disk. 
### Why SQSRV ###
SQSRV was written as a project to learn the GO language. It uses many of the features of the GO language including:

- goroutines for concurrency
- Channels for communication between goroutines
- Mutexes
- Network communication between client & server
- Interfaces
- First class functions
- Variadic functions
- gobs
- file handling
- Cross Platform (Linux, Windows)

## Windows Install ##

1.  Install go from https://golang.org/doc/install. Pick version 1.12.5 or newer
2. Install editor from https://code.visualstudio.com/ (Recommended but not necessary)
On the welcome screen of VisualStudio code under Customize/Tools & Languages, Select Go and install the customization packages
3. Install git from https://git-scm.com/downloads
4. At command prompt 
    ```
    mkdir %GOPATH%\src\github.com\wilphi
    cd %GOPATH%\src\github.com\wilphi
    git clone https://github.com/wilphi/sqsrv
    git clone https://github.com/wilphi/sqshell
    go get github.com/sirupsen/logrus
    mkdir %GOPATH%\src\github.com\wilphi\sqsrv\dbfiles
    ```
5. To build server project & run tests (sqsrv)
    ```
    cd %GOPATH%\src\github.com\wilphi\sqsrv
    go build
    go test ./...
    ```
    To see more detail use go test -v ./...
6. To build shell project & run tests (sqshell)
    ```
    cd %GOPATH%\src\github.com\wilphi\sqshell
    go build
    go test ./...
    ```
    To see more detail use go test -v ./...
## Linux Install ##
The linux install steps are almost the same except for some minor syntax changes for file paths and environment variables

## Using SQSRV & SQSHELL ##
To run the program
  ```
  cd %GOPATH%\src\github.com\wilphi\sqsrv
  sqsrv
  ```
and in a separate command prompt
```
cd %GOPATH%\src\github.com\wilphi\sqshell
sqshell
```

From the sqshell prompt you can either type in a SQL command, shell command or a server command. All commands must be on one line terminated by \n (Enter Key)
#### SQL Commands ###
The SQL commands supported by SQSRV are currently limited to very simple commands. Major limitations include:

  - Individual commands are atomic but there are no transactions
  - No user security
  - No Joins
  - From clauses are restricted to one table
  - No functions with the exception of a limited count ability
  - Where clauses have limited comparsion operators =, <, > and can use logical operators of AND, OR, NOT
  - No Indexes
  - No Group by or Having clauses
  - No Unions
  - No SELECT DISTINCT

See [syntax.md](./syntax.md) for more

#### Shell Commands ###
There are currently only two shell commands
* **@*filename***  reads SQL commands from specified file line by line and sends them to the server. Each SQL command must be on one line.
* **exit** terminates the sqshell. This does not affect the running sqsrv
#### Server Commands ###
All server commands can be seen by typing help at the sqshell prompt. Important commands include:

- **help** displays all server commands
- **shutdown** terminates the server
- **checkpoint** saves the database to disk
