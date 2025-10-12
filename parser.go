package mindb

import (
	"fmt"
	"regexp"
	"strings"
)

// Statement types
type StatementType int

const (
	CreateDatabase StatementType = iota
	DropDatabase
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
	CreateProcedure
	DropProcedure
	CallProcedure
	DescribeTable
	CreateUser
	DropUser
	AlterUser
	GrantPrivileges
	RevokePrivileges
	ShowGrants
	ShowUsers
	CreateRole
	DropRole
	GrantRole
	RevokeRole
	ShowRoles
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
	// Stored procedure fields
	ProcedureName string
	ProcedureCode []byte
	ProcedureLang string
	ProcedureArgs []interface{}
	ReturnType    string
	// User management fields
	Username    string
	Password    string
	Host        string
	Privileges  []string
	RoleName    string
	Description string
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
	case strings.HasPrefix(sqlUpper, "DROP DATABASE"):
		return p.parseDropDatabase(sql)
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
	case strings.HasPrefix(sqlUpper, "CREATE PROCEDURE"):
		return p.parseCreateProcedure(sql)
	case strings.HasPrefix(sqlUpper, "DROP PROCEDURE"):
		return p.parseDropProcedure(sql)
	case strings.HasPrefix(sqlUpper, "CALL"):
		return p.parseCallProcedure(sql)
	case strings.HasPrefix(sqlUpper, "DESCRIBE"), strings.HasPrefix(sqlUpper, "DESC "):
		return p.parseDescribeTable(sql)
	case strings.HasPrefix(sqlUpper, "CREATE USER"):
		return p.parseCreateUser(sql)
	case strings.HasPrefix(sqlUpper, "DROP USER"):
		return p.parseDropUser(sql)
	case strings.HasPrefix(sqlUpper, "GRANT"):
		return p.parseGrant(sql)
	case strings.HasPrefix(sqlUpper, "REVOKE"):
		return p.parseRevoke(sql)
	case strings.HasPrefix(sqlUpper, "SHOW GRANTS"):
		return p.parseShowGrants(sql)
	case strings.HasPrefix(sqlUpper, "SHOW USERS"):
		return p.parseShowUsers(sql)
	case strings.HasPrefix(sqlUpper, "ALTER USER"):
		return p.parseAlterUser(sql)
	case strings.HasPrefix(sqlUpper, "CREATE ROLE"):
		return p.parseCreateRole(sql)
	case strings.HasPrefix(sqlUpper, "DROP ROLE"):
		return p.parseDropRole(sql)
	case strings.HasPrefix(sqlUpper, "SHOW ROLES"):
		return p.parseShowRoles(sql)
	case strings.HasPrefix(sqlUpper, "GRANT") && strings.Contains(sqlUpper, "TO") && !strings.Contains(sqlUpper, "ON"):
		return p.parseGrantRole(sql)
	case strings.HasPrefix(sqlUpper, "REVOKE") && strings.Contains(sqlUpper, "FROM") && !strings.Contains(sqlUpper, "ON"):
		return p.parseRevokeRole(sql)
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

// parseDropDatabase parses DROP DATABASE statement
func (p *Parser) parseDropDatabase(sql string) (*Statement, error) {
	// Support IF EXISTS
	re := regexp.MustCompile(`(?i)DROP\s+DATABASE\s+(?:IF\s+EXISTS\s+)?(\w+)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid DROP DATABASE syntax")
	}

	ifExists := strings.Contains(strings.ToUpper(sql), "IF EXISTS")

	return &Statement{
		Type:     DropDatabase,
		Database: matches[1],
		IfExists: ifExists,
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

	// Parse values - support multiple value sets for batch insert
	// VALUES (1,'a'), (2,'b'), (3,'c')
	allValues := make([][]interface{}, 0)
	
	// Check if there are multiple value sets
	remainingAfterFirst := strings.TrimSpace(remainingSQL[valuesEnd+1:])
	if strings.HasPrefix(remainingAfterFirst, ",") {
		// Multiple value sets - parse them all
		fullValuesStr := remainingSQL[:valuesEnd+1]
		valueSets := p.splitValueSets(fullValuesStr)
		
		for _, valueSet := range valueSets {
			values, err := p.parseValues(valueSet)
			if err != nil {
				return nil, err
			}
			allValues = append(allValues, values)
		}
		
		// Update afterValues for RETURNING clause
		// Find where the value sets end
		lastParen := strings.LastIndex(remainingSQL, ")")
		if lastParen > valuesEnd {
			afterValues = strings.TrimSpace(remainingSQL[lastParen+1:])
			if strings.HasPrefix(strings.ToUpper(afterValues), "RETURNING") {
				returningStr = strings.TrimSpace(afterValues[9:])
			}
		}
	} else {
		// Single value set
		values, err := p.parseValues(valuesStr)
		if err != nil {
			return nil, err
		}
		allValues = append(allValues, values)
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
		Values:    allValues,
		Returning: returning,
	}, nil
}

// splitValueSets splits multiple value sets like "(1,'a'), (2,'b'), (3,'c')"
func (p *Parser) splitValueSets(valuesStr string) []string {
	result := make([]string, 0)
	var current strings.Builder
	parenDepth := 0
	inQuote := false
	var quoteChar rune
	
	for _, ch := range valuesStr {
		if !inQuote {
			if ch == '\'' || ch == '"' {
				inQuote = true
				quoteChar = ch
			} else if ch == '(' {
				parenDepth++
			} else if ch == ')' {
				parenDepth--
				current.WriteRune(ch)
				if parenDepth == 0 {
					// End of a value set
					valueSet := strings.TrimSpace(current.String())
					if len(valueSet) > 2 { // Must have at least "()"
						// Remove outer parentheses
						valueSet = strings.TrimSpace(valueSet[1 : len(valueSet)-1])
						result = append(result, valueSet)
					}
					current.Reset()
					continue
				}
			} else if ch == ',' && parenDepth == 0 {
				// Skip commas between value sets
				continue
			}
		} else {
			if ch == quoteChar {
				inQuote = false
			}
		}
		current.WriteRune(ch)
	}
	
	return result
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

// parseCreateProcedure parses CREATE PROCEDURE statement
func (p *Parser) parseCreateProcedure(sql string) (*Statement, error) {
	// Syntax: CREATE PROCEDURE name(params) RETURNS type LANGUAGE lang AS 'base64_code'
	re := regexp.MustCompile(`(?i)CREATE\s+PROCEDURE\s+(\w+)\s*\((.*?)\)\s+RETURNS\s+(\w+)\s+LANGUAGE\s+(\w+)\s+AS\s+'([^']+)'`)
	matches := re.FindStringSubmatch(sql)
	
	if len(matches) < 6 {
		return nil, fmt.Errorf("invalid CREATE PROCEDURE syntax")
	}
	
	stmt := &Statement{
		Type:          CreateProcedure,
		ProcedureName: matches[1],
		ReturnType:    matches[3],
		ProcedureLang: matches[4],
	}
	
	// Parse parameters
	if matches[2] != "" {
		params, err := p.parseColumnDefinitions(matches[2])
		if err != nil {
			return nil, fmt.Errorf("failed to parse parameters: %w", err)
		}
		stmt.Columns = params
	}
	
	// Decode base64 WASM code
	stmt.ProcedureCode = []byte(matches[5]) // Store as-is, will decode in engine
	
	return stmt, nil
}

// parseDropProcedure parses DROP PROCEDURE statement
func (p *Parser) parseDropProcedure(sql string) (*Statement, error) {
	re := regexp.MustCompile(`(?i)DROP\s+PROCEDURE\s+(?:IF\s+EXISTS\s+)?(\w+)`)
	matches := re.FindStringSubmatch(sql)
	
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid DROP PROCEDURE syntax")
	}
	
	stmt := &Statement{
		Type:          DropProcedure,
		ProcedureName: matches[1],
	}
	
	// Check for IF EXISTS
	if strings.Contains(strings.ToUpper(sql), "IF EXISTS") {
		stmt.IfExists = true
	}
	
	return stmt, nil
}

// parseCallProcedure parses CALL statement
func (p *Parser) parseCallProcedure(sql string) (*Statement, error) {
	// Syntax: CALL procedure_name(arg1, arg2, ...)
	re := regexp.MustCompile(`(?i)CALL\s+(\w+)\s*\((.*?)\)`)
	matches := re.FindStringSubmatch(sql)
	
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid CALL syntax")
	}
	
	stmt := &Statement{
		Type:          CallProcedure,
		ProcedureName: matches[1],
	}
	
	// Parse arguments
	if len(matches) >= 3 && matches[2] != "" {
		args, err := p.parseValues(matches[2])
		if err != nil {
			return nil, fmt.Errorf("failed to parse arguments: %w", err)
		}
		stmt.ProcedureArgs = args
	}
	
	return stmt, nil
}

// parseCreateUser parses CREATE USER statement
// Syntax: CREATE USER 'username'@'host' IDENTIFIED BY 'password';
func (p *Parser) parseCreateUser(sql string) (*Statement, error) {
	re := regexp.MustCompile(`(?i)CREATE\s+USER\s+'([^']+)'@'([^']+)'\s+IDENTIFIED\s+BY\s+'([^']+)'`)
	matches := re.FindStringSubmatch(sql)
	
	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid CREATE USER syntax")
	}
	
	return &Statement{
		Type:     CreateUser,
		Username: matches[1],
		Host:     matches[2],
		Password: matches[3],
	}, nil
}

// parseDropUser parses DROP USER statement
// Syntax: DROP USER 'username'@'host';
func (p *Parser) parseDropUser(sql string) (*Statement, error) {
	re := regexp.MustCompile(`(?i)DROP\s+USER\s+'([^']+)'@'([^']+)'`)
	matches := re.FindStringSubmatch(sql)
	
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid DROP USER syntax")
	}
	
	return &Statement{
		Type:     DropUser,
		Username: matches[1],
		Host:     matches[2],
	}, nil
}

// parseGrant parses GRANT statement
// Syntax: GRANT privileges ON database.table TO 'username'@'host';
func (p *Parser) parseGrant(sql string) (*Statement, error) {
	re := regexp.MustCompile(`(?i)GRANT\s+([\w\s,]+)\s+ON\s+([^.]+)\.([^\s]+)\s+TO\s+'([^']+)'@'([^']+)'`)
	matches := re.FindStringSubmatch(sql)
	
	if len(matches) < 6 {
		return nil, fmt.Errorf("invalid GRANT syntax")
	}
	
	// Parse privileges
	privStr := strings.ToUpper(strings.TrimSpace(matches[1]))
	var privileges []string
	if privStr == "ALL" || privStr == "ALL PRIVILEGES" {
		privileges = []string{"ALL"}
	} else {
		privs := strings.Split(privStr, ",")
		for _, p := range privs {
			privileges = append(privileges, strings.TrimSpace(p))
		}
	}
	
	return &Statement{
		Type:       GrantPrivileges,
		Privileges: privileges,
		Database:   matches[2],
		Table:      matches[3],
		Username:   matches[4],
		Host:       matches[5],
	}, nil
}

// parseRevoke parses REVOKE statement
// Syntax: REVOKE privileges ON database.table FROM 'username'@'host';
func (p *Parser) parseRevoke(sql string) (*Statement, error) {
	re := regexp.MustCompile(`(?i)REVOKE\s+([\w\s,]+)\s+ON\s+([^.]+)\.([^\s]+)\s+FROM\s+'([^']+)'@'([^']+)'`)
	matches := re.FindStringSubmatch(sql)
	
	if len(matches) < 6 {
		return nil, fmt.Errorf("invalid REVOKE syntax")
	}
	
	// Parse privileges
	privStr := strings.ToUpper(strings.TrimSpace(matches[1]))
	var privileges []string
	if privStr == "ALL" || privStr == "ALL PRIVILEGES" {
		privileges = []string{"ALL"}
	} else {
		privs := strings.Split(privStr, ",")
		for _, p := range privs {
			privileges = append(privileges, strings.TrimSpace(p))
		}
	}
	
	return &Statement{
		Type:       RevokePrivileges,
		Privileges: privileges,
		Database:   matches[2],
		Table:      matches[3],
		Username:   matches[4],
		Host:       matches[5],
	}, nil
}

// parseShowGrants parses SHOW GRANTS statement
// Syntax: SHOW GRANTS FOR 'username'@'host';
func (p *Parser) parseShowGrants(sql string) (*Statement, error) {
	re := regexp.MustCompile(`(?i)SHOW\s+GRANTS\s+FOR\s+'([^']+)'@'([^']+)'`)
	matches := re.FindStringSubmatch(sql)
	
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid SHOW GRANTS syntax")
	}
	
	return &Statement{
		Type:     ShowGrants,
		Username: matches[1],
		Host:     matches[2],
	}, nil
}

// parseShowUsers parses SHOW USERS statement
// Syntax: SHOW USERS;
func (p *Parser) parseShowUsers(sql string) (*Statement, error) {
	return &Statement{
		Type: ShowUsers,
	}, nil
}

// parseAlterUser parses ALTER USER statement
// Syntax: ALTER USER 'username'@'host' IDENTIFIED BY 'newpassword';
func (p *Parser) parseAlterUser(sql string) (*Statement, error) {
	re := regexp.MustCompile(`(?i)ALTER\s+USER\s+'([^']+)'@'([^']+)'\s+IDENTIFIED\s+BY\s+'([^']+)'`)
	matches := re.FindStringSubmatch(sql)
	
	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid ALTER USER syntax")
	}
	
	return &Statement{
		Type:     AlterUser,
		Username: matches[1],
		Host:     matches[2],
		Password: matches[3],
	}, nil
}

// parseCreateRole parses CREATE ROLE statement
// Syntax: CREATE ROLE 'rolename';
func (p *Parser) parseCreateRole(sql string) (*Statement, error) {
	re := regexp.MustCompile(`(?i)CREATE\s+ROLE\s+'([^']+)'`)
	matches := re.FindStringSubmatch(sql)
	
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid CREATE ROLE syntax")
	}
	
	return &Statement{
		Type:     CreateRole,
		RoleName: matches[1],
	}, nil
}

// parseDropRole parses DROP ROLE statement
// Syntax: DROP ROLE 'rolename';
func (p *Parser) parseDropRole(sql string) (*Statement, error) {
	re := regexp.MustCompile(`(?i)DROP\s+ROLE\s+'([^']+)'`)
	matches := re.FindStringSubmatch(sql)
	
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid DROP ROLE syntax")
	}
	
	return &Statement{
		Type:     DropRole,
		RoleName: matches[1],
	}, nil
}

// parseShowRoles parses SHOW ROLES statement
// Syntax: SHOW ROLES;
func (p *Parser) parseShowRoles(sql string) (*Statement, error) {
	return &Statement{
		Type: ShowRoles,
	}, nil
}

// parseGrantRole parses GRANT role TO user statement
// Syntax: GRANT 'rolename' TO 'username'@'host';
func (p *Parser) parseGrantRole(sql string) (*Statement, error) {
	re := regexp.MustCompile(`(?i)GRANT\s+'([^']+)'\s+TO\s+'([^']+)'@'([^']+)'`)
	matches := re.FindStringSubmatch(sql)
	
	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid GRANT role syntax")
	}
	
	return &Statement{
		Type:     GrantRole,
		RoleName: matches[1],
		Username: matches[2],
		Host:     matches[3],
	}, nil
}

// parseRevokeRole parses REVOKE role FROM user statement
// Syntax: REVOKE 'rolename' FROM 'username'@'host';
func (p *Parser) parseRevokeRole(sql string) (*Statement, error) {
	re := regexp.MustCompile(`(?i)REVOKE\s+'([^']+)'\s+FROM\s+'([^']+)'@'([^']+)'`)
	matches := re.FindStringSubmatch(sql)
	
	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid REVOKE role syntax")
	}
	
	return &Statement{
		Type:     RevokeRole,
		RoleName: matches[1],
		Username: matches[2],
		Host:     matches[3],
	}, nil
}

// parseDescribeTable parses DESCRIBE or DESC statement
func (p *Parser) parseDescribeTable(sql string) (*Statement, error) {
	// Match: DESCRIBE table_name or DESC table_name
	re := regexp.MustCompile(`(?i)(?:DESCRIBE|DESC)\s+(\w+)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid DESCRIBE syntax")
	}

	return &Statement{
		Type:  DescribeTable,
		Table: matches[1],
	}, nil
}
