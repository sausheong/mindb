package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
)

func main() {
	// Parse command-line flags
	dataDir := flag.String("data", "", "Data directory for persistent storage (default: in-memory only)")
	flag.Parse()

	fmt.Println("Mindb - A Minimalist SQL Database")
	fmt.Println("Version 2.0.0 - Production Edition")
	fmt.Println("Type 'exit' or 'quit' to exit")
	fmt.Println("Multi-line statements are supported - end with ';'")
	fmt.Println("Use arrow keys for history, Ctrl+A/E for line navigation")

	// Create engine adapter (uses PagedEngine with MVCC, WAL, and Catalog)
	var engine *EngineAdapter
	var err error

	// Default to current directory if no data directory specified
	if *dataDir == "" {
		*dataDir = "./mindb_data"
	}

	// Expand home directory if needed
	if strings.HasPrefix(*dataDir, "~/") {
		home, _ := os.UserHomeDir()
		*dataDir = filepath.Join(home, (*dataDir)[2:])
	}

	// Create engine with WAL enabled
	engine, err = NewEngineAdapter(*dataDir, true)
	if err != nil {
		fmt.Printf("Error initializing database: %v\n", err)
		os.Exit(1)
	}
	defer engine.Close()

	fmt.Printf("Database initialized: %s\n", *dataDir)
	fmt.Println("Features: MVCC, WAL Recovery, System Catalog")
	fmt.Println()

	parser := NewParser()

	// Check if stdin is a terminal or pipe
	stdinStat, _ := os.Stdin.Stat()
	isPiped := (stdinStat.Mode() & os.ModeCharDevice) == 0

	// Use basic mode for piped input, readline for interactive
	if isPiped {
		runBasicMode(engine, parser)
		return
	}

	// Configure readline for interactive mode
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "mindb> ",
		HistoryFile:     "/tmp/mindb_history.txt",
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		// Fallback to basic mode if readline fails
		runBasicMode(engine, parser)
		return
	}
	defer rl.Close()

	var statementBuffer strings.Builder
	isMultiLine := false

	for {
		// Set appropriate prompt
		if isMultiLine {
			rl.SetPrompt("    -> ")
		} else {
			rl.SetPrompt("mindb> ")
		}

		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				if statementBuffer.Len() == 0 {
					fmt.Println("Goodbye!")
					break
				} else {
					// Clear buffer on interrupt
					statementBuffer.Reset()
					isMultiLine = false
					continue
				}
			} else if err == io.EOF {
				fmt.Println("Goodbye!")
				break
			}
			continue
		}

		// Check for exit commands on empty buffer
		if !isMultiLine {
			trimmedLine := strings.TrimSpace(line)

			// Skip comment lines (lines starting with --)
			if strings.HasPrefix(trimmedLine, "--") {
				continue
			}

			if trimmedLine == "exit" || trimmedLine == "quit" {
				fmt.Println("Goodbye!")
				break
			}

			// Handle empty lines
			if trimmedLine == "" {
				continue
			}
		} else {
			// In multi-line mode, also skip comment lines
			trimmedLine := strings.TrimSpace(line)
			if strings.HasPrefix(trimmedLine, "--") {
				continue
			}
		}

		// Add line to buffer
		if statementBuffer.Len() > 0 {
			statementBuffer.WriteString(" ")
		}
		statementBuffer.WriteString(line)

		// Check if statement is complete (ends with semicolon)
		currentStatement := strings.TrimSpace(statementBuffer.String())
		if strings.HasSuffix(currentStatement, ";") {
			// Statement is complete, execute it
			input := currentStatement
			statementBuffer.Reset()
			isMultiLine = false

			// Execute the statement
			executeStatement(engine, parser, input)
		} else {
			// Statement is not complete, continue reading
			isMultiLine = true
		}
	}
}

// executeStatement executes a single SQL statement
func executeStatement(engine *EngineAdapter, parser *Parser, input string) {
	// Check for USE DATABASE command
	if strings.HasPrefix(strings.ToUpper(input), "USE ") {
		dbName := strings.TrimSpace(input[4:])
		dbName = strings.TrimSuffix(dbName, ";")
		if err := engine.UseDatabase(dbName); err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			fmt.Printf("Database changed to '%s'\n", dbName)
		}
		return
	}

	// Parse and execute statement
	stmt, err := parser.Parse(input)
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}

	result, err := engine.Execute(stmt)
	if err != nil {
		fmt.Printf("Execution error: %v\n", err)
		return
	}

	fmt.Println(result)
}

// runBasicMode runs without readline support (fallback)
func runBasicMode(engine *EngineAdapter, parser *Parser) {
	scanner := bufio.NewScanner(os.Stdin)
	var statementBuffer strings.Builder
	isMultiLine := false

	for scanner.Scan() {
		line := scanner.Text()

		// Check for exit commands on empty buffer
		if !isMultiLine {
			trimmedLine := strings.TrimSpace(line)

			// Skip comment lines
			if strings.HasPrefix(trimmedLine, "--") {
				continue
			}

			if trimmedLine == "exit" || trimmedLine == "quit" {
				break
			}

			if trimmedLine == "" {
				continue
			}
		} else {
			trimmedLine := strings.TrimSpace(line)
			if strings.HasPrefix(trimmedLine, "--") {
				continue
			}
		}

		// Add line to buffer
		if statementBuffer.Len() > 0 {
			statementBuffer.WriteString(" ")
		}
		statementBuffer.WriteString(line)

		// Check if statement is complete
		currentStatement := strings.TrimSpace(statementBuffer.String())
		if strings.HasSuffix(currentStatement, ";") {
			input := currentStatement
			statementBuffer.Reset()
			isMultiLine = false

			executeStatement(engine, parser, input)
		} else {
			isMultiLine = true
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
	}
}
