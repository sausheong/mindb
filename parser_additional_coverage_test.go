package mindb

import "testing"

func TestParseCreateDatabaseIfNotExists(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("CREATE DATABASE IF NOT EXISTS analytics")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if stmt.Type != CreateDatabase {
		t.Fatalf("Expected CreateDatabase, got %v", stmt.Type)
	}
	if stmt.Database != "analytics" {
		t.Fatalf("Expected database name analytics, got %s", stmt.Database)
	}
	if !stmt.IfNotExists {
		t.Fatal("Expected IfNotExists to be true")
	}
}

func TestParseDropDatabaseIfExists(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("DROP DATABASE IF EXISTS archive")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if stmt.Type != DropDatabase {
		t.Fatalf("Expected DropDatabase, got %v", stmt.Type)
	}
	if stmt.Database != "archive" {
		t.Fatalf("Expected database name archive, got %s", stmt.Database)
	}
	if !stmt.IfExists {
		t.Fatal("Expected IfExists to be true")
	}
}

func TestParseCreateTableWithSchema(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("CREATE TABLE IF NOT EXISTS public.users (id INT PRIMARY KEY, status VARCHAR)")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if stmt.Type != CreateTable {
		t.Fatalf("Expected CreateTable, got %v", stmt.Type)
	}
	if stmt.Schema != "public" {
		t.Fatalf("Expected schema public, got %s", stmt.Schema)
	}
	if stmt.Table != "users" {
		t.Fatalf("Expected table users, got %s", stmt.Table)
	}
	if !stmt.IfNotExists {
		t.Fatal("Expected IfNotExists to be true")
	}
	if len(stmt.Columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(stmt.Columns))
	}
	if !stmt.Columns[0].PrimaryKey {
		t.Fatal("Expected first column to be primary key")
	}
}

func TestParseAlterTableIfExists(t *testing.T) {
	parser := NewParser()
	stmt, err := parser.Parse("ALTER TABLE IF EXISTS public.users ADD COLUMN last_login TIMESTAMP")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if stmt.Type != AlterTable {
		t.Fatalf("Expected AlterTable, got %v", stmt.Type)
	}
	if stmt.Schema != "public" {
		t.Fatalf("Expected schema public, got %s", stmt.Schema)
	}
	if stmt.Table != "users" {
		t.Fatalf("Expected table users, got %s", stmt.Table)
	}
	if !stmt.IfExists {
		t.Fatal("Expected IfExists to be true")
	}
	if stmt.NewColumn.Name != "last_login" {
		t.Fatalf("Expected new column last_login, got %s", stmt.NewColumn.Name)
	}
	if stmt.NewColumn.DataType != "TIMESTAMP" {
		t.Fatalf("Expected data type TIMESTAMP, got %s", stmt.NewColumn.DataType)
	}
}

func TestParseSelectWithSchemaLimitOffset(t *testing.T) {
	parser := NewParser()
	sql := "SELECT name FROM public.users WHERE age > 30 ORDER BY age DESC LIMIT 5 OFFSET 10"
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if stmt.Type != Select {
		t.Fatalf("Expected Select, got %v", stmt.Type)
	}
	if stmt.Schema != "public" {
		t.Fatalf("Expected schema public, got %s", stmt.Schema)
	}
	if stmt.Table != "users" {
		t.Fatalf("Expected table users, got %s", stmt.Table)
	}
	if len(stmt.Columns) != 1 || stmt.Columns[0].Name != "name" {
		t.Fatalf("Expected column name, got %+v", stmt.Columns)
	}
	if stmt.OrderBy != "age" || !stmt.OrderDesc {
		t.Fatal("Expected ORDER BY age DESC")
	}
	if stmt.Limit != 5 {
		t.Fatalf("Expected limit 5, got %d", stmt.Limit)
	}
	if stmt.Offset != 10 {
		t.Fatalf("Expected offset 10, got %d", stmt.Offset)
	}
}

func TestParseInsertRequiresColumnList(t *testing.T) {
	parser := NewParser()
	sql := "INSERT INTO public.events VALUES (1, 'A')"
	_, err := parser.Parse(sql)
	if err == nil {
		t.Fatal("Expected error for INSERT without column list")
	}
}

func TestParseInsertWithReturningSingleRow(t *testing.T) {
	parser := NewParser()
	sql := "INSERT INTO public.events (id, name) VALUES (1, 'A') RETURNING id"
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if stmt.Type != Insert {
		t.Fatalf("Expected Insert, got %v", stmt.Type)
	}
	if stmt.Schema != "public" {
		t.Fatalf("Expected schema public, got %s", stmt.Schema)
	}
	if stmt.Table != "events" {
		t.Fatalf("Expected table events, got %s", stmt.Table)
	}
	if len(stmt.Values) != 1 {
		t.Fatalf("Expected one value set, got %d", len(stmt.Values))
	}
	if len(stmt.Returning) != 1 || stmt.Returning[0] != "id" {
		t.Fatalf("Expected RETURNING id, got %+v", stmt.Returning)
	}
}

func TestParseUpdateWithReturning(t *testing.T) {
	parser := NewParser()
	sql := "UPDATE public.users SET status = 'active' WHERE id = 1 RETURNING id"
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if stmt.Type != Update {
		t.Fatalf("Expected Update, got %v", stmt.Type)
	}
	if stmt.Schema != "public" {
		t.Fatalf("Expected schema public, got %s", stmt.Schema)
	}
	if len(stmt.Returning) != 1 || stmt.Returning[0] != "id" {
		t.Fatalf("Expected RETURNING id, got %+v", stmt.Returning)
	}
}

func TestParseDeleteWithReturning(t *testing.T) {
	parser := NewParser()
	sql := "DELETE FROM public.users WHERE id = 1 RETURNING id"
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if stmt.Type != Delete {
		t.Fatalf("Expected Delete, got %v", stmt.Type)
	}
	if stmt.Schema != "public" {
		t.Fatalf("Expected schema public, got %s", stmt.Schema)
	}
	if len(stmt.Conditions) != 1 {
		t.Fatalf("Expected one condition, got %d", len(stmt.Conditions))
	}
	if len(stmt.Returning) != 1 || stmt.Returning[0] != "id" {
		t.Fatalf("Expected RETURNING id, got %+v", stmt.Returning)
	}
}
