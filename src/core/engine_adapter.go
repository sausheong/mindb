package mindb

import (
	"fmt"
	"strings"
)

// EngineAdapter wraps PagedEngine to provide backward compatibility with the old Engine interface
type EngineAdapter struct {
	pagedEngine *PagedEngine
}

// NewEngineAdapter creates a new adapter wrapping PagedEngine
func NewEngineAdapter(dataDir string, enableWAL bool) (*EngineAdapter, error) {
	pagedEngine, err := NewPagedEngineWithWAL(dataDir, enableWAL)
	if err != nil {
		return nil, err
	}

	return &EngineAdapter{
		pagedEngine: pagedEngine,
	}, nil
}

// Execute executes a parsed statement (backward compatibility interface)
func (ea *EngineAdapter) Execute(stmt *Statement) (string, error) {
	// Check permissions (except for user management commands which have their own checks)
	if stmt.Type != CreateUser && stmt.Type != DropUser && 
	   stmt.Type != GrantPrivileges && stmt.Type != RevokePrivileges && 
	   stmt.Type != ShowGrants {
		if err := ea.checkPermission(stmt); err != nil {
			return "", err
		}
	}
	
	switch stmt.Type {
	case BeginTransaction:
		return ea.beginTransaction()
	case CommitTransaction:
		return ea.commitTransaction()
	case RollbackTransaction:
		return ea.rollbackTransaction()
	case CreateDatabase:
		return ea.createDatabase(stmt)
	case DropDatabase:
		return ea.dropDatabase(stmt)
	case CreateTable:
		return ea.createTable(stmt)
	case AlterTable:
		return ea.alterTable(stmt)
	case DropTable:
		return ea.dropTable(stmt)
	case Insert:
		return ea.insertData(stmt)
	case Select:
		return ea.selectData(stmt)
	case Update:
		return ea.updateData(stmt)
	case Delete:
		return ea.deleteData(stmt)
	case DescribeTable:
		return ea.describeTable(stmt)
	case CallProcedure:
		return ea.callProcedure(stmt)
	case CreateUser:
		return ea.createUser(stmt)
	case DropUser:
		return ea.dropUser(stmt)
	case GrantPrivileges:
		return ea.grantPrivileges(stmt)
	case RevokePrivileges:
		return ea.revokePrivileges(stmt)
	case ShowGrants:
		return ea.showGrants(stmt)
	case ShowUsers:
		return ea.showUsers(stmt)
	case AlterUser:
		return ea.alterUser(stmt)
	case CreateRole:
		return ea.createRole(stmt)
	case DropRole:
		return ea.dropRole(stmt)
	case GrantRole:
		return ea.grantRole(stmt)
	case RevokeRole:
		return ea.revokeRole(stmt)
	case ShowRoles:
		return ea.showRoles(stmt)
	default:
		return "", fmt.Errorf("unsupported statement type")
	}
}

// UseDatabase switches to a database
func (ea *EngineAdapter) UseDatabase(name string) error {
	return ea.pagedEngine.UseDatabase(name)
}

// GetWASMEngine returns the WASM engine for introspection
func (ea *EngineAdapter) GetWASMEngine() *WASMEngine {
	return ea.pagedEngine.GetWASMEngine()
}

// Close closes the engine
func (ea *EngineAdapter) Close() error {
	return ea.pagedEngine.Close()
}

// createDatabase creates a new database
func (ea *EngineAdapter) createDatabase(stmt *Statement) (string, error) {
	if err := ea.pagedEngine.CreateDatabase(stmt.Database); err != nil {
		return "", err
	}
	return fmt.Sprintf("Database '%s' created successfully", stmt.Database), nil
}

// dropDatabase drops a database
func (ea *EngineAdapter) dropDatabase(stmt *Statement) (string, error) {
	if err := ea.pagedEngine.DropDatabase(stmt.Database); err != nil {
		return "", err
	}
	return fmt.Sprintf("Database '%s' dropped successfully", stmt.Database), nil
}

// createTable creates a new table
func (ea *EngineAdapter) createTable(stmt *Statement) (string, error) {
	tableName := stmt.Table
	if stmt.Schema != "" {
		tableName = stmt.Schema + "." + stmt.Table
	}

	if err := ea.pagedEngine.CreateTable(tableName, stmt.Columns); err != nil {
		return "", err
	}

	return fmt.Sprintf("Table '%s' created successfully", tableName), nil
}

// alterTable alters a table
func (ea *EngineAdapter) alterTable(stmt *Statement) (string, error) {
	// Get current table to retrieve existing columns
	db, err := ea.pagedEngine.getCurrentDatabase()
	if err != nil {
		return "", err
	}

	db.mu.RLock()
	table, exists := db.Tables[stmt.Table]
	db.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", stmt.Table)
	}

	// Add new column to existing columns
	newColumns := append(table.Columns, stmt.Columns...)

	// Update catalog
	if err := ea.pagedEngine.catalog.AlterTable(db.Name, stmt.Table, newColumns); err != nil {
		return "", err
	}

	// Update in-memory structure
	table.mu.Lock()
	table.Columns = newColumns
	table.mu.Unlock()

	// Save catalog
	if err := ea.pagedEngine.catalog.SaveCatalog(); err != nil {
		return "", fmt.Errorf("failed to save catalog: %v", err)
	}

	return fmt.Sprintf("Table '%s' altered successfully", stmt.Table), nil
}

// dropTable drops a table
func (ea *EngineAdapter) dropTable(stmt *Statement) (string, error) {
	if err := ea.pagedEngine.DropTable(stmt.Table); err != nil {
		if stmt.IfExists {
			return fmt.Sprintf("Table '%s' does not exist, skipping", stmt.Table), nil
		}
		return "", err
	}

	return fmt.Sprintf("Table '%s' dropped successfully", stmt.Table), nil
}

// insertData inserts data into a table
func (ea *EngineAdapter) insertData(stmt *Statement) (string, error) {
	rowCount := 0

	for _, values := range stmt.Values {
		if len(values) != len(stmt.Columns) {
			return "", fmt.Errorf("column count doesn't match value count")
		}

		row := make(Row)
		for i, col := range stmt.Columns {
			row[col.Name] = values[i]
		}

		if err := ea.pagedEngine.InsertRow(stmt.Table, row); err != nil {
			return "", err
		}
		rowCount++
	}

	return fmt.Sprintf("%d row(s) inserted", rowCount), nil
}

// selectData selects data from a table
func (ea *EngineAdapter) selectData(stmt *Statement) (string, error) {
	// Handle JOINs
	if len(stmt.Joins) > 0 {
		return ea.selectDataWithJoin(stmt)
	}

	// Handle aggregates
	if len(stmt.Aggregates) > 0 {
		return ea.selectDataWithAggregates(stmt)
	}

	// Convert Statement conditions to PagedEngine conditions
	var conditions []Condition
	for _, cond := range stmt.Conditions {
		conditions = append(conditions, Condition{
			Column:   cond.Column,
			Operator: cond.Operator,
			Value:    cond.Value,
		})
	}

	rows, err := ea.pagedEngine.SelectRows(stmt.Table, conditions)
	if err != nil {
		return "", err
	}

	// Apply LIMIT and OFFSET
	if stmt.Offset > 0 {
		if stmt.Offset >= len(rows) {
			rows = []Row{}
		} else {
			rows = rows[stmt.Offset:]
		}
	}

	if stmt.Limit > 0 && stmt.Limit < len(rows) {
		rows = rows[:stmt.Limit]
	}

	// Format output
	return ea.formatSelectResult(stmt, rows)
}

// selectDataWithJoin executes a SELECT with JOIN
func (ea *EngineAdapter) selectDataWithJoin(stmt *Statement) (string, error) {
	// Get rows from left table
	leftRows, err := ea.pagedEngine.SelectRows(stmt.Table, nil)
	if err != nil {
		return "", err
	}

	// Execute each join
	joinExecutor := NewJoinExecutor()
	result := leftRows

	for _, join := range stmt.Joins {
		// Get rows from right table
		rightRows, err := ea.pagedEngine.SelectRows(join.Table, nil)
		if err != nil {
			return "", err
		}

		// Execute join
		result, err = joinExecutor.ExecuteJoin(result, rightRows, join)
		if err != nil {
			return "", err
		}
	}

	// Apply WHERE conditions on joined result
	if len(stmt.Conditions) > 0 {
		result = ea.filterRows(result, stmt.Conditions)
	}

	// Apply LIMIT and OFFSET
	if stmt.Offset > 0 && stmt.Offset < len(result) {
		result = result[stmt.Offset:]
	}
	if stmt.Limit > 0 && stmt.Limit < len(result) {
		result = result[:stmt.Limit]
	}

	return ea.formatSelectResult(stmt, result)
}

// selectDataWithAggregates executes a SELECT with aggregate functions
func (ea *EngineAdapter) selectDataWithAggregates(stmt *Statement) (string, error) {
	// Get all rows
	var conditions []Condition
	for _, cond := range stmt.Conditions {
		conditions = append(conditions, Condition{
			Column:   cond.Column,
			Operator: cond.Operator,
			Value:    cond.Value,
		})
	}

	rows, err := ea.pagedEngine.SelectRows(stmt.Table, conditions)
	if err != nil {
		return "", err
	}

	// Execute aggregates
	aggExecutor := NewAggregateExecutor()
	result, err := aggExecutor.ExecuteAggregates(rows, stmt.Aggregates, stmt.GroupBy)
	if err != nil {
		return "", err
	}

	// Apply HAVING filter if present
	if len(stmt.Having) > 0 {
		result = ea.filterRows(result, stmt.Having)
	}

	return ea.formatSelectResult(stmt, result)
}

// filterRows filters rows based on conditions
func (ea *EngineAdapter) filterRows(rows []Row, conditions []Condition) []Row {
	filtered := make([]Row, 0)

	for _, row := range rows {
		match := true
		for _, cond := range conditions {
			val, exists := row[cond.Column]
			if !exists {
				match = false
				break
			}

			switch cond.Operator {
			case "=":
				if val != cond.Value {
					match = false
				}
			case "!=":
				if val == cond.Value {
					match = false
				}
			case ">":
				if CompareValues(val, cond.Value) <= 0 {
					match = false
				}
			case "<":
				if CompareValues(val, cond.Value) >= 0 {
					match = false
				}
			case ">=":
				if CompareValues(val, cond.Value) < 0 {
					match = false
				}
			case "<=":
				if CompareValues(val, cond.Value) > 0 {
					match = false
				}
			}

			if !match {
				break
			}
		}

		if match {
			filtered = append(filtered, row)
		}
	}

	return filtered
}

// updateData updates data in a table
func (ea *EngineAdapter) updateData(stmt *Statement) (string, error) {
	// Convert Statement conditions to PagedEngine conditions
	var conditions []Condition
	for _, cond := range stmt.Conditions {
		conditions = append(conditions, Condition{
			Column:   cond.Column,
			Operator: cond.Operator,
			Value:    cond.Value,
		})
	}

	count, err := ea.pagedEngine.UpdateRows(stmt.Table, stmt.Updates, conditions)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%d row(s) updated", count), nil
}

// deleteData deletes data from a table
func (ea *EngineAdapter) deleteData(stmt *Statement) (string, error) {
	// Convert Statement conditions to PagedEngine conditions
	var conditions []Condition
	for _, cond := range stmt.Conditions {
		conditions = append(conditions, Condition{
			Column:   cond.Column,
			Operator: cond.Operator,
			Value:    cond.Value,
		})
	}

	count, err := ea.pagedEngine.DeleteRows(stmt.Table, conditions)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%d row(s) deleted", count), nil
}

// formatSelectResult formats the SELECT result as a table
func (ea *EngineAdapter) formatSelectResult(stmt *Statement, rows []Row) (string, error) {
	if len(rows) == 0 {
		return "0 rows returned", nil
	}

	// Determine columns to display
	var displayColumns []string
	
	// For aggregates, use aggregate aliases
	if len(stmt.Aggregates) > 0 {
		aggExecutor := NewAggregateExecutor()
		for _, agg := range stmt.Aggregates {
			alias := agg.Alias
			if alias == "" {
				alias = aggExecutor.getDefaultAlias(agg)
			}
			displayColumns = append(displayColumns, alias)
		}
		// Add GROUP BY column if present
		if stmt.GroupBy != "" {
			displayColumns = append([]string{stmt.GroupBy}, displayColumns...)
		}
	} else if len(stmt.Joins) > 0 {
		// For JOINs, get columns from first row (sorted for consistency)
		colMap := make(map[string]bool)
		for colName := range rows[0] {
			colMap[colName] = true
		}
		for colName := range colMap {
			displayColumns = append(displayColumns, colName)
		}
	} else {
		// Get table to access column definitions
		db, err := ea.pagedEngine.getCurrentDatabase()
		if err != nil {
			return "", err
		}

		db.mu.RLock()
		table, exists := db.Tables[stmt.Table]
		db.mu.RUnlock()

		if !exists {
			return "", fmt.Errorf("table '%s' does not exist", stmt.Table)
		}

		// Determine columns from statement or table
		if len(stmt.Columns) == 0 || (len(stmt.Columns) == 1 && stmt.Columns[0].Name == "*") {
			displayColumns = make([]string, len(table.Columns))
			for i, col := range table.Columns {
				displayColumns[i] = col.Name
			}
		} else {
			displayColumns = make([]string, len(stmt.Columns))
			for i, col := range stmt.Columns {
				displayColumns[i] = col.Name
			}
		}
	}

	// Calculate column widths
	colWidths := make(map[string]int)
	for _, colName := range displayColumns {
		colWidths[colName] = len(colName)
	}

	for _, row := range rows {
		for _, colName := range displayColumns {
			if val, ok := row[colName]; ok && val != nil {
				valStr := fmt.Sprintf("%v", val)
				if len(valStr) > colWidths[colName] {
					colWidths[colName] = len(valStr)
				}
			}
		}
	}

	// Build result string
	var result strings.Builder

	// Header
	result.WriteString("+")
	for _, colName := range displayColumns {
		result.WriteString(strings.Repeat("-", colWidths[colName]+2))
		result.WriteString("+")
	}
	result.WriteString("\n|")

	for _, colName := range displayColumns {
		result.WriteString(fmt.Sprintf(" %-*s |", colWidths[colName], colName))
	}
	result.WriteString("\n+")

	for _, colName := range displayColumns {
		result.WriteString(strings.Repeat("-", colWidths[colName]+2))
		result.WriteString("+")
	}
	result.WriteString("\n")

	// Rows
	for _, row := range rows {
		result.WriteString("|")
		for _, colName := range displayColumns {
			val := row[colName]
			valStr := ""
			if val != nil {
				valStr = fmt.Sprintf("%v", val)
			}
			result.WriteString(fmt.Sprintf(" %-*s |", colWidths[colName], valStr))
		}
		result.WriteString("\n")
	}

	// Footer
	result.WriteString("+")
	for _, colName := range displayColumns {
		result.WriteString(strings.Repeat("-", colWidths[colName]+2))
		result.WriteString("+")
	}
	result.WriteString(fmt.Sprintf("\n%d row(s) returned", len(rows)))

	return result.String(), nil
}

// beginTransaction starts a new explicit transaction
func (ea *EngineAdapter) beginTransaction() (string, error) {
	if err := ea.pagedEngine.BeginTransaction(); err != nil {
		return "", err
	}
	return "Transaction started", nil
}

// commitTransaction commits the current transaction
func (ea *EngineAdapter) commitTransaction() (string, error) {
	if err := ea.pagedEngine.CommitTransaction(); err != nil {
		return "", err
	}
	return "Transaction committed", nil
}

// rollbackTransaction rolls back the current transaction
func (ea *EngineAdapter) rollbackTransaction() (string, error) {
	if err := ea.pagedEngine.RollbackTransaction(); err != nil {
		return "", err
	}
	return "Transaction rolled back", nil
}

// CreateProcedureViaAdapter creates a stored procedure
func (ea *EngineAdapter) CreateProcedureViaAdapter(proc *StoredProcedure) error {
	return ea.pagedEngine.CreateProcedure(proc)
}

// DropProcedureViaAdapter drops a stored procedure
func (ea *EngineAdapter) DropProcedureViaAdapter(name string) error {
	return ea.pagedEngine.DropProcedure(name)
}

// ListProceduresViaAdapter lists all stored procedures
func (ea *EngineAdapter) ListProceduresViaAdapter() []*StoredProcedure {
	return ea.pagedEngine.ListProcedures()
}

// CallProcedureViaAdapter calls a stored procedure
func (ea *EngineAdapter) CallProcedureViaAdapter(name string, args ...interface{}) (interface{}, error) {
	return ea.pagedEngine.CallProcedure(name, args...)
}

// describeTable returns the schema of a table in table format
func (ea *EngineAdapter) describeTable(stmt *Statement) (string, error) {
	// Get current database
	db, err := ea.pagedEngine.getCurrentDatabase()
	if err != nil {
		return "", err
	}

	// Get table metadata
	db.mu.RLock()
	table, exists := db.Tables[stmt.Table]
	db.mu.RUnlock()

	if !exists {
		return "", fmt.Errorf("table '%s' does not exist", stmt.Table)
	}

	// Format table schema in the same format as SELECT results
	// This allows parseResultString to parse it correctly
	var result strings.Builder
	
	// Header separator
	result.WriteString("+--------------------+---------------+----------+----------+----------+\n")
	
	// Column headers
	result.WriteString("| Column             | Type          | Null     | Key      | Default  |\n")
	
	// Header separator
	result.WriteString("+--------------------+---------------+----------+----------+----------+\n")

	// Data rows
	for _, col := range table.Columns {
		nullStr := "YES"
		if col.NotNull {
			nullStr = "NO"
		}
		
		keyStr := ""
		if col.PrimaryKey {
			keyStr = "PRI"
		} else if col.Unique {
			keyStr = "UNI"
		}
		
		defaultStr := "NULL"
		if col.Default != nil {
			defaultStr = fmt.Sprintf("%v", col.Default)
		}
		
		result.WriteString(fmt.Sprintf("| %-18s | %-13s | %-8s | %-8s | %-8s |\n", 
			col.Name, col.DataType, nullStr, keyStr, defaultStr))
	}

	// Footer separator
	result.WriteString("+--------------------+---------------+----------+----------+----------+\n")
	
	// Row count
	result.WriteString(fmt.Sprintf("%d row(s) returned\n", len(table.Columns)))

	return result.String(), nil
}

// callProcedure executes a stored procedure
func (ea *EngineAdapter) callProcedure(stmt *Statement) (string, error) {
	// Call the procedure
	result, err := ea.pagedEngine.CallProcedure(stmt.ProcedureName, stmt.ProcedureArgs...)
	if err != nil {
		return "", fmt.Errorf("procedure call failed: %w", err)
	}

	// Format the result as a table for display
	var output strings.Builder
	
	// Header
	output.WriteString("+------------------+\n")
	output.WriteString("| result           |\n")
	output.WriteString("+------------------+\n")
	
	// Result value
	output.WriteString(fmt.Sprintf("| %-16v |\n", result))
	
	// Footer
	output.WriteString("+------------------+\n")
	output.WriteString("1 row(s) returned\n")
	
	return output.String(), nil
}

// createUser creates a new user
func (ea *EngineAdapter) createUser(stmt *Statement) (string, error) {
	err := ea.pagedEngine.userManager.CreateUser(stmt.Username, stmt.Password, stmt.Host)
	if err != nil {
		return "", err
	}
	
	// Save users to disk
	if err := ea.pagedEngine.userManager.Save(); err != nil {
		// Log error but don't fail the operation
		fmt.Printf("Warning: failed to save users: %v\n", err)
	}
	
	return fmt.Sprintf("User '%s'@'%s' created successfully", stmt.Username, stmt.Host), nil
}

// dropUser drops a user
func (ea *EngineAdapter) dropUser(stmt *Statement) (string, error) {
	err := ea.pagedEngine.userManager.DropUser(stmt.Username, stmt.Host)
	if err != nil {
		return "", err
	}
	
	// Save users to disk
	if err := ea.pagedEngine.userManager.Save(); err != nil {
		fmt.Printf("Warning: failed to save users: %v\n", err)
	}
	
	return fmt.Sprintf("User '%s'@'%s' dropped successfully", stmt.Username, stmt.Host), nil
}

// grantPrivileges grants privileges to a user
func (ea *EngineAdapter) grantPrivileges(stmt *Statement) (string, error) {
	// Convert string privileges to Privilege type
	privileges := make([]Privilege, len(stmt.Privileges))
	for i, p := range stmt.Privileges {
		privileges[i] = Privilege(p)
	}
	
	err := ea.pagedEngine.userManager.GrantPrivileges(
		stmt.Username, stmt.Host, stmt.Database, stmt.Table, privileges)
	if err != nil {
		return "", err
	}
	
	// Save users to disk
	if err := ea.pagedEngine.userManager.Save(); err != nil {
		fmt.Printf("Warning: failed to save users: %v\n", err)
	}
	
	return fmt.Sprintf("Granted %v on %s.%s to '%s'@'%s'", 
		stmt.Privileges, stmt.Database, stmt.Table, stmt.Username, stmt.Host), nil
}

// revokePrivileges revokes privileges from a user
func (ea *EngineAdapter) revokePrivileges(stmt *Statement) (string, error) {
	// Convert string privileges to Privilege type
	privileges := make([]Privilege, len(stmt.Privileges))
	for i, p := range stmt.Privileges {
		privileges[i] = Privilege(p)
	}
	
	err := ea.pagedEngine.userManager.RevokePrivileges(
		stmt.Username, stmt.Host, stmt.Database, stmt.Table, privileges)
	if err != nil {
		return "", err
	}
	
	// Save users to disk
	if err := ea.pagedEngine.userManager.Save(); err != nil {
		fmt.Printf("Warning: failed to save users: %v\n", err)
	}
	
	return fmt.Sprintf("Revoked %v on %s.%s from '%s'@'%s'", 
		stmt.Privileges, stmt.Database, stmt.Table, stmt.Username, stmt.Host), nil
}

// showGrants shows grants for a user
func (ea *EngineAdapter) showGrants(stmt *Statement) (string, error) {
	grants := ea.pagedEngine.userManager.ListGrants(stmt.Username, stmt.Host)
	
	if len(grants) == 0 {
		return fmt.Sprintf("No grants for '%s'@'%s'", stmt.Username, stmt.Host), nil
	}
	
	var output strings.Builder
	output.WriteString(fmt.Sprintf("Grants for '%s'@'%s':\n", stmt.Username, stmt.Host))
	output.WriteString("+--------------------------------------------------+\n")
	
	for _, grant := range grants {
		privs := make([]string, len(grant.Privileges))
		for i, p := range grant.Privileges {
			privs[i] = string(p)
		}
		
		grantStr := fmt.Sprintf("GRANT %s ON %s.%s TO '%s'@'%s'",
			strings.Join(privs, ", "),
			grant.Database,
			grant.Table,
			grant.Username,
			grant.Host)
		
		output.WriteString(fmt.Sprintf("| %-48s |\n", grantStr))
	}
	
	output.WriteString("+--------------------------------------------------+\n")
	return output.String(), nil
}

// showUsers shows all users in the system
func (ea *EngineAdapter) showUsers(stmt *Statement) (string, error) {
	users := ea.pagedEngine.userManager.ListUsers()
	
	if len(users) == 0 {
		return "No users found", nil
	}
	
	var output strings.Builder
	output.WriteString("+----------------------+----------------------+---------------------+\n")
	output.WriteString("| User                 | Host                 | Created             |\n")
	output.WriteString("+----------------------+----------------------+---------------------+\n")
	
	for _, user := range users {
		output.WriteString(fmt.Sprintf("| %-20s | %-20s | %-19s |\n",
			truncate(user.Username, 20),
			truncate(user.Host, 20),
			user.CreatedAt.Format("2006-01-02 15:04:05")))
	}
	
	output.WriteString("+----------------------+----------------------+---------------------+\n")
	output.WriteString(fmt.Sprintf("%d user(s) found\n", len(users)))
	
	return output.String(), nil
}

// truncate truncates a string to a maximum length
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// checkPermission checks if the current user has permission to execute a statement
func (ea *EngineAdapter) checkPermission(stmt *Statement) error {
	// Get current user
	user := ea.pagedEngine.currentUser
	if user == "" {
		user = "root@%" // Default to root if not set
	}
	
	parts := strings.Split(user, "@")
	if len(parts) != 2 {
		return fmt.Errorf("invalid user format: %s", user)
	}
	username, host := parts[0], parts[1]
	
	// Root user has all privileges
	if username == "root" {
		return nil
	}
	
	// Determine required privilege based on statement type
	var requiredPriv Privilege
	database := stmt.Database
	table := stmt.Table
	
	// Use current database if not specified
	if database == "" {
		database = ea.pagedEngine.currentDB
	}
	
	switch stmt.Type {
	case Select, DescribeTable:
		requiredPriv = PrivilegeSelect
	case Insert:
		requiredPriv = PrivilegeInsert
	case Update:
		requiredPriv = PrivilegeUpdate
	case Delete:
		requiredPriv = PrivilegeDelete
	case CreateTable, CreateDatabase:
		requiredPriv = PrivilegeCreate
		if table == "" {
			table = "*"
		}
	case DropTable, DropDatabase:
		requiredPriv = PrivilegeDrop
		if table == "" {
			table = "*"
		}
	case AlterTable:
		requiredPriv = PrivilegeCreate // ALTER requires CREATE privilege
	case CallProcedure:
		requiredPriv = PrivilegeSelect // Calling procedures requires SELECT
	default:
		// For unknown types, allow (backward compatibility)
		return nil
	}
	
	// Check if user has the required privilege
	if !ea.pagedEngine.userManager.HasPrivilege(username, host, database, table, requiredPriv) {
		return fmt.Errorf("access denied for user '%s'@'%s' (missing %s privilege on %s.%s)", 
			username, host, requiredPriv, database, table)
	}
	
	return nil
}

// SetCurrentUser sets the current authenticated user
func (ea *EngineAdapter) SetCurrentUser(username, host string) {
	ea.pagedEngine.currentUser = fmt.Sprintf("%s@%s", username, host)
}

// Authenticate authenticates a user
func (ea *EngineAdapter) Authenticate(username, password, host string) bool {
	return ea.pagedEngine.userManager.Authenticate(username, password, host)
}

// LogLoginSuccess logs a successful login
func (ea *EngineAdapter) LogLoginSuccess(username, host string) {
	if ea.pagedEngine.auditLogger != nil {
		ea.pagedEngine.auditLogger.LogLoginSuccess(username, host)
	}
}

// LogLoginFailed logs a failed login attempt
func (ea *EngineAdapter) LogLoginFailed(username, host, reason string) {
	if ea.pagedEngine.auditLogger != nil {
		ea.pagedEngine.auditLogger.LogLoginFailed(username, host, reason)
	}
}

// IsAccountLocked checks if an account is locked
func (ea *EngineAdapter) IsAccountLocked(username, host string) bool {
	return ea.pagedEngine.userManager.IsLocked(username, host)
}

// alterUser changes a user's password
func (ea *EngineAdapter) alterUser(stmt *Statement) (string, error) {
	err := ea.pagedEngine.userManager.ChangePassword(stmt.Username, stmt.Host, stmt.Password)
	if err != nil {
		return "", err
	}
	
	// Save users to disk
	if err := ea.pagedEngine.userManager.Save(); err != nil {
		fmt.Printf("Warning: failed to save users: %v\n", err)
	}
	
	return fmt.Sprintf("Password changed for '%s'@'%s'", stmt.Username, stmt.Host), nil
}

// createRole creates a new role
func (ea *EngineAdapter) createRole(stmt *Statement) (string, error) {
	err := ea.pagedEngine.userManager.CreateRole(stmt.RoleName, "")
	if err != nil {
		return "", err
	}
	
	// Save users to disk
	if err := ea.pagedEngine.userManager.Save(); err != nil {
		fmt.Printf("Warning: failed to save users: %v\n", err)
	}
	
	return fmt.Sprintf("Role '%s' created successfully", stmt.RoleName), nil
}

// dropRole removes a role
func (ea *EngineAdapter) dropRole(stmt *Statement) (string, error) {
	err := ea.pagedEngine.userManager.DropRole(stmt.RoleName)
	if err != nil {
		return "", err
	}
	
	// Save users to disk
	if err := ea.pagedEngine.userManager.Save(); err != nil {
		fmt.Printf("Warning: failed to save users: %v\n", err)
	}
	
	return fmt.Sprintf("Role '%s' dropped successfully", stmt.RoleName), nil
}

// grantRole assigns a role to a user
func (ea *EngineAdapter) grantRole(stmt *Statement) (string, error) {
	err := ea.pagedEngine.userManager.GrantRoleToUser(stmt.Username, stmt.Host, stmt.RoleName)
	if err != nil {
		return "", err
	}
	
	// Save users to disk
	if err := ea.pagedEngine.userManager.Save(); err != nil {
		fmt.Printf("Warning: failed to save users: %v\n", err)
	}
	
	return fmt.Sprintf("Role '%s' granted to '%s'@'%s'", stmt.RoleName, stmt.Username, stmt.Host), nil
}

// revokeRole removes a role from a user
func (ea *EngineAdapter) revokeRole(stmt *Statement) (string, error) {
	err := ea.pagedEngine.userManager.RevokeRoleFromUser(stmt.Username, stmt.Host, stmt.RoleName)
	if err != nil {
		return "", err
	}
	
	// Save users to disk
	if err := ea.pagedEngine.userManager.Save(); err != nil {
		fmt.Printf("Warning: failed to save users: %v\n", err)
	}
	
	return fmt.Sprintf("Role '%s' revoked from '%s'@'%s'", stmt.RoleName, stmt.Username, stmt.Host), nil
}

// showRoles shows all roles in the system
func (ea *EngineAdapter) showRoles(stmt *Statement) (string, error) {
	roles := ea.pagedEngine.userManager.ListRoles()
	
	if len(roles) == 0 {
		return "No roles found", nil
	}
	
	var output strings.Builder
	output.WriteString("+----------------------+------------------------------------------+---------------------+\n")
	output.WriteString("| Role                 | Description                              | Created             |\n")
	output.WriteString("+----------------------+------------------------------------------+---------------------+\n")
	
	for _, role := range roles {
		desc := role.Description
		if desc == "" {
			desc = "-"
		}
		output.WriteString(fmt.Sprintf("| %-20s | %-40s | %-19s |\n",
			truncate(role.Name, 20),
			truncate(desc, 40),
			role.CreatedAt.Format("2006-01-02 15:04:05")))
	}
	
	output.WriteString("+----------------------+------------------------------------------+---------------------+\n")
	output.WriteString(fmt.Sprintf("%d role(s) found\n", len(roles)))
	
	return output.String(), nil
}
