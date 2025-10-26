package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/chzyer/readline"
)

type QueryRequest struct {
	SQL string `json:"sql"`
}

type QueryResponse struct {
	Columns   []string        `json:"columns"`
	Rows      [][]interface{} `json:"rows"`
	RowCount  int             `json:"row_count"`
	Truncated bool            `json:"truncated"`
	LatencyMS int64           `json:"latency_ms"`
}

type ExecuteRequest struct {
	SQL string `json:"sql"`
}

type ExecuteResponse struct {
	AffectedRows int            `json:"affected_rows"`
	Returning    *QueryResponse `json:"returning,omitempty"`
	LatencyMS    int64          `json:"latency_ms"`
}

type ErrorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func main() {
	// Parse command-line flags
	serverURL := flag.String("server", "http://localhost:8080", "Server URL for mindb")
	flag.Parse()

	fmt.Println("Mindb CLI - Client for Mindb Server")
	fmt.Printf("Connected to: %s\n", *serverURL)
	fmt.Println("Type 'exit' or 'quit' to exit")
	fmt.Println("Multi-line statements are supported - end with ';'")
	fmt.Println("Use arrow keys for history, Ctrl+A/E for line navigation")
	fmt.Println()

	client := &http.Client{}

	rl, err := readline.New("mindb> ")
	if err != nil {
		fmt.Printf("Error initializing readline: %v\n", err)
		os.Exit(1)
	}
	defer rl.Close()

	var multiline strings.Builder

	for {
		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				if multiline.Len() > 0 {
					multiline.Reset()
					fmt.Println("^C")
					continue
				}
				break
			}
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading input: %v\n", err)
			continue
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		multiline.WriteString(line)
		multiline.WriteString(" ")

		// Check if statement ends with semicolon
		if !strings.HasSuffix(strings.TrimSpace(line), ";") {
			continue
		}

		sql := strings.TrimSpace(multiline.String())
		multiline.Reset()

		if sql == "" {
			continue
		}

		// Handle special commands
		if strings.ToLower(sql) == "exit;" || strings.ToLower(sql) == "quit;" {
			break
		}

		// Execute the SQL
		err = executeSQL(client, *serverURL, sql)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}
		fmt.Println()
	}

	fmt.Println("Goodbye!")
}

func executeSQL(client *http.Client, serverURL, sql string) error {
	// Determine if it's a SELECT query (read-only) or other (execute)
	isSelect := strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT")

	var url string
	var reqBody interface{}
	if isSelect {
		url = serverURL + "/query"
		reqBody = QueryRequest{SQL: sql}
	} else {
		url = serverURL + "/execute"
		reqBody = ExecuteRequest{SQL: sql}
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", url, strings.NewReader(string(jsonData)))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp ErrorResponse
		if err := json.Unmarshal(body, &errResp); err != nil {
			return fmt.Errorf("server error (%d): %s", resp.StatusCode, string(body))
		}
		return fmt.Errorf("server error: %s - %s", errResp.Error.Code, errResp.Error.Message)
	}

	if isSelect {
		var queryResp QueryResponse
		if err := json.Unmarshal(body, &queryResp); err != nil {
			return fmt.Errorf("failed to parse query response: %w", err)
		}
		displayQueryResults(&queryResp)
	} else {
		var execResp ExecuteResponse
		if err := json.Unmarshal(body, &execResp); err != nil {
			return fmt.Errorf("failed to parse execute response: %w", err)
		}
		displayExecuteResults(&execResp)
	}

	return nil
}

func displayQueryResults(resp *QueryResponse) {
	if len(resp.Columns) == 0 {
		fmt.Printf("Query executed successfully (%d ms)\n", resp.LatencyMS)
		return
	}

	// Print columns
	for i, col := range resp.Columns {
		if i > 0 {
			fmt.Print(" | ")
		}
		fmt.Print(col)
	}
	fmt.Println()

	// Print separator
	for i := range resp.Columns {
		if i > 0 {
			fmt.Print("-|-")
		}
		fmt.Print(strings.Repeat("-", len(resp.Columns[i])))
	}
	fmt.Println()

	// Print rows
	for _, row := range resp.Rows {
		for i, val := range row {
			if i > 0 {
				fmt.Print(" | ")
			}
			fmt.Print(val)
		}
		fmt.Println()
	}

	fmt.Printf("\n%d rows returned (%d ms)\n", resp.RowCount, resp.LatencyMS)
	if resp.Truncated {
		fmt.Println("Warning: Results were truncated")
	}
}

func displayExecuteResults(resp *ExecuteResponse) {
	fmt.Printf("Query executed successfully. Affected rows: %d (%d ms)\n", resp.AffectedRows, resp.LatencyMS)
	if resp.Returning != nil {
		fmt.Println("Returning:")
		displayQueryResults(resp.Returning)
	}
}
