package main

import (
	"fmt"
	"regexp"
	"strings"
)

// Statement types
type StatementType int

const (
	CreateDatabase StatementType = iota
	CreateTable
	AlterTable
	DropTable
	Select
	Insert
	Update
	Delete
	BeginTransaction
	CommitTransaction
	RollbackTransaction
	Unknown
)

// Statement represents a parsed SQL statement
type Statement struct {
	Type        StatementType
	Database    string
	Table       string
	Schema      string
	Columns     []Column
	Values      [][]interface{}
	Conditions  []Condition
	Updates     map[string]interface{}
	NewColumn   Column
	OrderBy     string
	OrderDesc   bool
	GroupBy     string
	Having      []Condition
	Limit       int
	Offset      int
	IfExists    bool
	IfNotExists bool
	Returning   []string
	Joins       []JoinClause
	Aggregates  []AggregateFunc
	Subquery    *Statement
}

// Column represents a table column
type Column struct {
	Name            string
	DataType        string
	PrimaryKey      bool
	NotNull         bool
	Default         interface{}
	Unique          bool
	ForeignKey      *ForeignKeyDef
}

// ForeignKeyDef represents a foreign key constraint
type ForeignKeyDef struct {
	RefTable    string
	RefColumn   string
	OnDelete    string // CASCADE, SET NULL, RESTRICT
	OnUpdate    string // CASCADE, SET NULL, RESTRICT
}

// Condition represents a WHERE clause condition
type Condition struct {
	Column   string
	Operator string
	Value    interface{}
}

// Parser parses SQL statements
type Parser struct{}

// NewParser creates a new parser
func NewParser() *Parser {
	return &Parser{}
}

// removeInlineComments removes SQL comments from a statement
func (p *Parser) removeInlineComments(sql string) string {
	var result strings.Builder
	inQuote := false
	quoteChar := rune(0)
	
	for i := 0; i < len(sql); i++ {
		ch := rune(sql[i])
		
		if !inQuote {
			// Check for quote start
			if ch == '\'' || ch == '"' {
				inQuote = true
				quoteChar = ch
				result.WriteRune(ch)
			} else if ch == '-' && i+1 < len(sql) && sql[i+1] == '-' {
				// Found comment start, skip to end of line or end of string
				for i < len(sql) && sql[i] != '\n' {
					i++
				}
				if i < len(sql) {
					result.WriteRune('\n') // Keep the newline
				}
			} else {
				result.WriteRune(ch)
			}
		} else {
			// Inside quote
			result.WriteRune(ch)
			if ch == quoteChar {
				// Check if it's escaped
				if i == 0 || sql[i-1] != '\\' {
					inQuote = false
				}
			}
		}
	}
	
	return result.String()
}

// Parse parses a SQL statement
func (p *Parser) Parse(sql string) (*Statement, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, fmt.Errorf("empty statement")
	}

	// Remove inline comments (-- to end of line)
	sql = p.removeInlineComments(sql)

	// Remove trailing semicolon
	sql = strings.TrimSuffix(sql, ";")

	// Convert to uppercase for keyword matching
	sqlUpper := strings.ToUpper(sql)

	switch {
	case strings.HasPrefix(sqlUpper, "BEGIN"):
		return &Statement{Type: BeginTransaction}, nil
	case strings.HasPrefix(sqlUpper, "COMMIT"):
		return &Statement{Type: CommitTransaction}, nil
	case strings.HasPrefix(sqlUpper, "ROLLBACK"):
		return &Statement{Type: RollbackTransaction}, nil
	case strings.HasPrefix(sqlUpper, "CREATE DATABASE"):
		return p.parseCreateDatabase(sql)
	case strings.HasPrefix(sqlUpper, "CREATE TABLE"):
		return p.parseCreateTable(sql)
	case strings.HasPrefix(sqlUpper, "ALTER TABLE"):
		return p.parseAlterTable(sql)
	case strings.HasPrefix(sqlUpper, "DROP TABLE"):
		return p.parseDropTable(sql)
	case strings.HasPrefix(sqlUpper, "SELECT"):
		return p.parseSelect(sql)
	case strings.HasPrefix(sqlUpper, "INSERT INTO"):
		return p.parseInsert(sql)
	case strings.HasPrefix(sqlUpper, "UPDATE"):
		return p.parseUpdate(sql)
	case strings.HasPrefix(sqlUpper, "DELETE FROM"):
		return p.parseDelete(sql)
	default:
		return &Statement{Type: Unknown}, fmt.Errorf("unknown statement type")
	}
}

// parseCreateDatabase parses CREATE DATABASE statement
func (p *Parser) parseCreateDatabase(sql string) (*Statement, error) {
	// Support IF NOT EXISTS
	re := regexp.MustCompile(`(?i)CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid CREATE DATABASE syntax")
	}

	ifNotExists := strings.Contains(strings.ToUpper(sql), "IF NOT EXISTS")

	return &Statement{
		Type:        CreateDatabase,
		Database:    matches[1],
		IfNotExists: ifNotExists,
	}, nil
}

// parseCreateTable parses CREATE TABLE statement
func (p *Parser) parseCreateTable(sql string) (*Statement, error) {
	// Support IF NOT EXISTS and schema-qualified names
	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)\s*\((.*)\)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax")
	}

	schema := matches[1]
	tableName := matches[2]
	columnsStr := matches[3]
	ifNotExists := strings.Contains(strings.ToUpper(sql), "IF NOT EXISTS")

	columns, err := p.parseColumnDefinitions(columnsStr)
	if err != nil {
		return nil, err
	}

	return &Statement{
		Type:        CreateTable,
		Schema:      schema,
		Table:       tableName,
		Columns:     columns,
		IfNotExists: ifNotExists,
	}, nil
}

// parseAlterTable parses ALTER TABLE statement
func (p *Parser) parseAlterTable(sql string) (*Statement, error) {
	// Support schema-qualified names and IF EXISTS
	re := regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)\s+ADD\s+(?:COLUMN\s+)?(\w+)\s+(\w+(?:\(\d+\))?)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 5 {
		return nil, fmt.Errorf("invalid ALTER TABLE syntax")
	}

	schema := matches[1]
	tableName := matches[2]
	ifExists := strings.Contains(strings.ToUpper(sql), "IF EXISTS")

	return &Statement{
		Type:     AlterTable,
		Schema:   schema,
		Table:    tableName,
		IfExists: ifExists,
		NewColumn: Column{
			Name:     matches[3],
			DataType: matches[4],
		},
	}, nil
}

// parseDropTable parses DROP TABLE statement
func (p *Parser) parseDropTable(sql string) (*Statement, error) {
	// Support IF EXISTS and schema-qualified names
	re := regexp.MustCompile(`(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid DROP TABLE syntax")
	}

	schema := matches[1]
	tableName := matches[2]
	ifExists := strings.Contains(strings.ToUpper(sql), "IF EXISTS")

	return &Statement{
		Type:     DropTable,
		Schema:   schema,
		Table:    tableName,
		IfExists: ifExists,
	}, nil
}

// parseSelect parses SELECT statement
func (p *Parser) parseSelect(sql string) (*Statement, error) {
	stmt := &Statement{Type: Select}

	// Check for JOINs
	if strings.Contains(strings.ToUpper(sql), "JOIN") {
		return p.parseSelectWithJoin(sql)
	}

	// Extract table name (support schema-qualified names)
	fromRe := regexp.MustCompile(`(?i)FROM\s+(?:(\w+)\.)?(\w+)`)
	fromMatches := fromRe.FindStringSubmatch(sql)
	if len(fromMatches) < 3 {
		return nil, fmt.Errorf("invalid SELECT syntax: missing FROM clause")
	}
	stmt.Schema = fromMatches[1]
	stmt.Table = fromMatches[2]

	// Extract columns
	selectRe := regexp.MustCompile(`(?i)SELECT\s+(.*?)\s+FROM`)
	selectMatches := selectRe.FindStringSubmatch(sql)
	if len(selectMatches) < 2 {
		return nil, fmt.Errorf("invalid SELECT syntax")
	}

	columnsStr := strings.TrimSpace(selectMatches[1])
	
	// Check for aggregate functions
	if p.hasAggregateFunctions(columnsStr) {
		aggregates, err := p.parseAggregateFunctions(columnsStr)
		if err != nil {
			return nil, err
		}
		stmt.Aggregates = aggregates
	} else if columnsStr != "*" {
		columnNames := strings.Split(columnsStr, ",")
		for _, col := range columnNames {
			stmt.Columns = append(stmt.Columns, Column{Name: strings.TrimSpace(col)})
		}
	}

	// Extract WHERE clause
	whereRe := regexp.MustCompile(`(?i)WHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+GROUP\s+BY|$)`)
	whereMatches := whereRe.FindStringSubmatch(sql)
	if len(whereMatches) >= 2 {
		conditions, err := p.parseConditions(whereMatches[1])
		if err != nil {
			return nil, err
		}
		stmt.Conditions = conditions
	}

	// Extract ORDER BY clause
	orderRe := regexp.MustCompile(`(?i)ORDER\s+BY\s+(\w+)(?:\s+(ASC|DESC))?`)
	orderMatches := orderRe.FindStringSubmatch(sql)
	if len(orderMatches) >= 2 {
		stmt.OrderBy = orderMatches[1]
		if len(orderMatches) >= 3 && strings.ToUpper(orderMatches[2]) == "DESC" {
			stmt.OrderDesc = true
		}
	}

	// Extract GROUP BY clause
	groupRe := regexp.MustCompile(`(?i)GROUP\s+BY\s+(\w+)`)
	groupMatches := groupRe.FindStringSubmatch(sql)
	if len(groupMatches) >= 2 {
		stmt.GroupBy = groupMatches[1]
	}

	// Extract LIMIT clause
	limitRe := regexp.MustCompile(`(?i)LIMIT\s+(\d+)`)
	limitMatches := limitRe.FindStringSubmatch(sql)
	if len(limitMatches) >= 2 {
		fmt.Sscanf(limitMatches[1], "%d", &stmt.Limit)
	}

	// Extract OFFSET clause
	offsetRe := regexp.MustCompile(`(?i)OFFSET\s+(\d+)`)
	offsetMatches := offsetRe.FindStringSubmatch(sql)
	if len(offsetMatches) >= 2 {
		fmt.Sscanf(offsetMatches[1], "%d", &stmt.Offset)
	}

	return stmt, nil
}

// parseInsert parses INSERT statement
func (p *Parser) parseInsert(sql string) (*Statement, error) {
	// Extract table name and schema
	tableRe := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(?:(\w+)\.)?(\w+)`)
	tableMatches := tableRe.FindStringSubmatch(sql)
	if len(tableMatches) < 3 {
		return nil, fmt.Errorf("invalid INSERT syntax: missing table name")
	}

	schema := tableMatches[1]
	tableName := tableMatches[2]

	// Extract columns - find content between first pair of parentheses after table name
	columnsRe := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(?:\w+\.)?\w+\s*\(([^)]+)\)`)
	columnsMatches := columnsRe.FindStringSubmatch(sql)
	if len(columnsMatches) < 2 {
		return nil, fmt.Errorf("invalid INSERT syntax: missing columns")
	}
	columnsStr := columnsMatches[1]

	// Extract values - find content between parentheses after VALUES keyword
	// Use a more robust approach to handle nested quotes and special characters
	valuesIdx := strings.Index(strings.ToUpper(sql), "VALUES")
	if valuesIdx == -1 {
		return nil, fmt.Errorf("invalid INSERT syntax: missing VALUES clause")
	}

	remainingSQL := sql[valuesIdx+6:] // Skip "VALUES"
	remainingSQL = strings.TrimSpace(remainingSQL)

	// Find the VALUES parentheses content
	if !strings.HasPrefix(remainingSQL, "(") {
		return nil, fmt.Errorf("invalid INSERT syntax: VALUES must be followed by parentheses")
	}

	// Find matching closing parenthesis, accounting for nested quotes
	parenDepth := 0
	inQuote := false
	quoteChar := rune(0)
	valuesEnd := -1

	for i, ch := range remainingSQL {
		if !inQuote {
			if ch == '\'' || ch == '"' {
				inQuote = true
				quoteChar = ch
			} else if ch == '(' {
				parenDepth++
			} else if ch == ')' {
				parenDepth--
				if parenDepth == 0 {
					valuesEnd = i
					break
				}
			}
		} else {
			if ch == quoteChar {
				// Check if it's escaped
				if i > 0 && remainingSQL[i-1] != '\\' {
					inQuote = false
				}
			}
		}
	}

	if valuesEnd == -1 {
		return nil, fmt.Errorf("invalid INSERT syntax: unclosed VALUES parentheses")
	}

	valuesStr := remainingSQL[1:valuesEnd] // Skip opening and closing parens

	// Check for RETURNING clause
	returningStr := ""
	afterValues := strings.TrimSpace(remainingSQL[valuesEnd+1:])
	if strings.HasPrefix(strings.ToUpper(afterValues), "RETURNING") {
		returningStr = strings.TrimSpace(afterValues[9:]) // Skip "RETURNING"
	}

	// Parse columns
	columnNames := strings.Split(columnsStr, ",")
	var columns []Column
	for _, col := range columnNames {
		columns = append(columns, Column{Name: strings.TrimSpace(col)})
	}

	// Parse values
	values, err := p.parseValues(valuesStr)
	if err != nil {
		return nil, err
	}

	// Parse RETURNING clause
	var returning []string
	if returningStr != "" {
		returnCols := strings.Split(returningStr, ",")
		for _, col := range returnCols {
			returning = append(returning, strings.TrimSpace(col))
		}
	}

	return &Statement{
		Type:      Insert,
		Schema:    schema,
		Table:     tableName,
		Columns:   columns,
		Values:    [][]interface{}{values},
		Returning: returning,
	}, nil
}

// parseUpdate parses UPDATE statement
func (p *Parser) parseUpdate(sql string) (*Statement, error) {
	stmt := &Statement{Type: Update, Updates: make(map[string]interface{})}

	// Extract table name (support schema-qualified names)
	tableRe := regexp.MustCompile(`(?i)UPDATE\s+(?:(\w+)\.)?(\w+)`)
	tableMatches := tableRe.FindStringSubmatch(sql)
	if len(tableMatches) < 3 {
		return nil, fmt.Errorf("invalid UPDATE syntax")
	}
	stmt.Schema = tableMatches[1]
	stmt.Table = tableMatches[2]

	// Extract SET clause
	setRe := regexp.MustCompile(`(?i)SET\s+(.*?)(?:\s+WHERE|$)`)
	setMatches := setRe.FindStringSubmatch(sql)
	if len(setMatches) < 2 {
		return nil, fmt.Errorf("invalid UPDATE syntax: missing SET clause")
	}

	// Parse SET assignments
	assignments := strings.Split(setMatches[1], ",")
	for _, assignment := range assignments {
		parts := strings.Split(assignment, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid SET assignment")
		}
		column := strings.TrimSpace(parts[0])
		value := p.parseValue(strings.TrimSpace(parts[1]))
		stmt.Updates[column] = value
	}

	// Extract WHERE clause
	whereRe := regexp.MustCompile(`(?i)WHERE\s+(.+?)(?:\s+RETURNING|$)`)
	whereMatches := whereRe.FindStringSubmatch(sql)
	if len(whereMatches) >= 2 {
		conditions, err := p.parseConditions(whereMatches[1])
		if err != nil {
			return nil, err
		}
		stmt.Conditions = conditions
	}

	// Extract RETURNING clause
	returningRe := regexp.MustCompile(`(?i)RETURNING\s+(.+)$`)
	returningMatches := returningRe.FindStringSubmatch(sql)
	if len(returningMatches) >= 2 {
		returnCols := strings.Split(returningMatches[1], ",")
		for _, col := range returnCols {
			stmt.Returning = append(stmt.Returning, strings.TrimSpace(col))
		}
	}

	return stmt, nil
}

// parseDelete parses DELETE statement
func (p *Parser) parseDelete(sql string) (*Statement, error) {
	stmt := &Statement{Type: Delete}

	// Extract table name (support schema-qualified names)
	tableRe := regexp.MustCompile(`(?i)DELETE\s+FROM\s+(?:(\w+)\.)?(\w+)`)
	tableMatches := tableRe.FindStringSubmatch(sql)
	if len(tableMatches) < 3 {
		return nil, fmt.Errorf("invalid DELETE syntax")
	}
	stmt.Schema = tableMatches[1]
	stmt.Table = tableMatches[2]

	// Extract WHERE clause
	whereRe := regexp.MustCompile(`(?i)WHERE\s+(.+?)(?:\s+RETURNING|$)`)
	whereMatches := whereRe.FindStringSubmatch(sql)
	if len(whereMatches) >= 2 {
		conditions, err := p.parseConditions(whereMatches[1])
		if err != nil {
			return nil, err
		}
		stmt.Conditions = conditions
	}

	// Extract RETURNING clause
	returningRe := regexp.MustCompile(`(?i)RETURNING\s+(.+)$`)
	returningMatches := returningRe.FindStringSubmatch(sql)
	if len(returningMatches) >= 2 {
		returnCols := strings.Split(returningMatches[1], ",")
		for _, col := range returnCols {
			stmt.Returning = append(stmt.Returning, strings.TrimSpace(col))
		}
	}

	return stmt, nil
}

// parseColumnDefinitions parses column definitions
func (p *Parser) parseColumnDefinitions(columnsStr string) ([]Column, error) {
	var columns []Column
	columnDefs := p.splitColumnDefinitions(columnsStr)

	for _, colDef := range columnDefs {
		colDefUpper := strings.ToUpper(colDef)
		parts := strings.Fields(strings.TrimSpace(colDef))
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid column definition: %s", colDef)
		}

		col := Column{
			Name:     parts[0],
			DataType: parts[1],
		}

		// Parse constraints
		if strings.Contains(colDefUpper, "PRIMARY KEY") {
			col.PrimaryKey = true
		}
		if strings.Contains(colDefUpper, "NOT NULL") {
			col.NotNull = true
		}
		if strings.Contains(colDefUpper, "UNIQUE") {
			col.Unique = true
		}

		// Parse DEFAULT value
		defaultRe := regexp.MustCompile(`(?i)DEFAULT\s+([^,]+)`)
		defaultMatches := defaultRe.FindStringSubmatch(colDef)
		if len(defaultMatches) >= 2 {
			defaultVal := strings.TrimSpace(defaultMatches[1])
			col.Default = p.parseValue(defaultVal)
		}

		// Parse FOREIGN KEY
		fkRe := regexp.MustCompile(`(?i)REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)(?:\s+ON\s+DELETE\s+(CASCADE|SET\s+NULL|RESTRICT))?(?:\s+ON\s+UPDATE\s+(CASCADE|SET\s+NULL|RESTRICT))?`)
		fkMatches := fkRe.FindStringSubmatch(colDef)
		if len(fkMatches) >= 3 {
			fk := &ForeignKeyDef{
				RefTable:  fkMatches[1],
				RefColumn: fkMatches[2],
				OnDelete:  "RESTRICT", // Default
				OnUpdate:  "RESTRICT", // Default
			}
			if len(fkMatches) >= 4 && fkMatches[3] != "" {
				fk.OnDelete = strings.ToUpper(strings.ReplaceAll(fkMatches[3], " ", "_"))
			}
			if len(fkMatches) >= 5 && fkMatches[4] != "" {
				fk.OnUpdate = strings.ToUpper(strings.ReplaceAll(fkMatches[4], " ", "_"))
			}
			col.ForeignKey = fk
		}

		columns = append(columns, col)
	}

	return columns, nil
}

// splitColumnDefinitions splits column definitions, handling nested parentheses
func (p *Parser) splitColumnDefinitions(columnsStr string) []string {
	var result []string
	var current strings.Builder
	parenDepth := 0

	for _, ch := range columnsStr {
		switch ch {
		case '(':
			parenDepth++
			current.WriteRune(ch)
		case ')':
			parenDepth--
			current.WriteRune(ch)
		case ',':
			if parenDepth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// parseConditions parses WHERE conditions
func (p *Parser) parseConditions(condStr string) ([]Condition, error) {
	var conditions []Condition

	// Simple condition parsing (supports single condition for now)
	re := regexp.MustCompile(`(\w+)\s*(=|!=|>|<|>=|<=)\s*(.+)`)
	matches := re.FindStringSubmatch(strings.TrimSpace(condStr))
	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid WHERE condition")
	}

	conditions = append(conditions, Condition{
		Column:   matches[1],
		Operator: matches[2],
		Value:    p.parseValue(strings.TrimSpace(matches[3])),
	})

	return conditions, nil
}

// parseValues parses a comma-separated list of values
func (p *Parser) parseValues(valuesStr string) ([]interface{}, error) {
	var values []interface{}
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for i, ch := range valuesStr {
		if !inQuote {
			if ch == '\'' || ch == '"' {
				inQuote = true
				quoteChar = ch
				current.WriteRune(ch)
			} else if ch == ',' {
				// End of value
				val := strings.TrimSpace(current.String())
				if val != "" {
					values = append(values, p.parseValue(val))
				}
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		} else {
			current.WriteRune(ch)
			if ch == quoteChar {
				// Check if it's escaped
				if i > 0 && valuesStr[i-1] != '\\' {
					inQuote = false
				}
			}
		}
	}

	// Add last value
	val := strings.TrimSpace(current.String())
	if val != "" {
		values = append(values, p.parseValue(val))
	}

	return values, nil
}

// parseValue parses a single value (string or number)
func (p *Parser) parseValue(val string) interface{} {
	// Remove quotes for strings
	if (strings.HasPrefix(val, "'") && strings.HasSuffix(val, "'")) ||
		(strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"")) {
		return val[1 : len(val)-1]
	}

	// Try to parse as number
	var num int
	if _, err := fmt.Sscanf(val, "%d", &num); err == nil {
		return num
	}

	var fnum float64
	if _, err := fmt.Sscanf(val, "%f", &fnum); err == nil {
		return fnum
	}

	// Return as string
	return val
}

// parseSelectWithJoin parses SELECT with JOIN
func (p *Parser) parseSelectWithJoin(sql string) (*Statement, error) {
	stmt := &Statement{Type: Select, Joins: make([]JoinClause, 0)}

	// Extract main table
	fromRe := regexp.MustCompile(`(?i)FROM\s+(\w+)(?:\s+(\w+))?`)
	fromMatches := fromRe.FindStringSubmatch(sql)
	if len(fromMatches) < 2 {
		return nil, fmt.Errorf("invalid SELECT syntax: missing FROM clause")
	}
	stmt.Table = fromMatches[1]

	// Extract columns
	selectRe := regexp.MustCompile(`(?i)SELECT\s+(.*?)\s+FROM`)
	selectMatches := selectRe.FindStringSubmatch(sql)
	if len(selectMatches) >= 2 {
		columnsStr := strings.TrimSpace(selectMatches[1])
		if columnsStr != "*" {
			columnNames := strings.Split(columnsStr, ",")
			for _, col := range columnNames {
				stmt.Columns = append(stmt.Columns, Column{Name: strings.TrimSpace(col)})
			}
		}
	}

	// Parse JOINs
	joinRe := regexp.MustCompile(`(?i)(INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN|JOIN)\s+(\w+)(?:\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+))?`)
	joinMatches := joinRe.FindAllStringSubmatch(sql, -1)

	for _, match := range joinMatches {
		if len(match) < 3 {
			continue
		}

		joinType := InnerJoin
		joinTypeStr := strings.ToUpper(strings.TrimSpace(match[1]))
		switch joinTypeStr {
		case "LEFT JOIN":
			joinType = LeftJoin
		case "RIGHT JOIN":
			joinType = RightJoin
		case "FULL JOIN":
			joinType = FullJoin
		case "CROSS JOIN":
			joinType = CrossJoin
		}

		join := JoinClause{
			Type:       joinType,
			Table:      match[2],
			OnOperator: "=",
		}

		if len(match) >= 5 && match[3] != "" {
			join.OnLeft = match[3]
			join.OnRight = match[4]
		}

		stmt.Joins = append(stmt.Joins, join)
	}

	// Extract WHERE clause
	whereRe := regexp.MustCompile(`(?i)WHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+GROUP\s+BY|$)`)
	whereMatches := whereRe.FindStringSubmatch(sql)
	if len(whereMatches) >= 2 {
		conditions, err := p.parseConditions(whereMatches[1])
		if err != nil {
			return nil, err
		}
		stmt.Conditions = conditions
	}

	return stmt, nil
}

// hasAggregateFunctions checks if a column string contains aggregate functions
func (p *Parser) hasAggregateFunctions(columnsStr string) bool {
	upper := strings.ToUpper(columnsStr)
	return strings.Contains(upper, "COUNT(") ||
		strings.Contains(upper, "SUM(") ||
		strings.Contains(upper, "AVG(") ||
		strings.Contains(upper, "MIN(") ||
		strings.Contains(upper, "MAX(")
}

// parseAggregateFunctions parses aggregate functions from column string
func (p *Parser) parseAggregateFunctions(columnsStr string) ([]AggregateFunc, error) {
	aggregates := make([]AggregateFunc, 0)

	// Match aggregate functions
	aggRe := regexp.MustCompile(`(?i)(COUNT|SUM|AVG|MIN|MAX)\s*\(\s*(\*|\w+)\s*\)(?:\s+AS\s+(\w+))?`)
	matches := aggRe.FindAllStringSubmatch(columnsStr, -1)

	for _, match := range matches {
		if len(match) < 3 {
			continue
		}

		funcType := CountFunc
		funcName := strings.ToUpper(match[1])
		switch funcName {
		case "SUM":
			funcType = SumFunc
		case "AVG":
			funcType = AvgFunc
		case "MIN":
			funcType = MinFunc
		case "MAX":
			funcType = MaxFunc
		}

		agg := AggregateFunc{
			Type:   funcType,
			Column: match[2],
		}

		if len(match) >= 4 && match[3] != "" {
			agg.Alias = match[3]
		}

		aggregates = append(aggregates, agg)
	}

	return aggregates, nil
}
