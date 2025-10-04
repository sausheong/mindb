package main

import (
	"testing"
)

func TestForeignKeyConstraints(t *testing.T) {
	tmpDir := t.TempDir()
	
	engine, err := NewPagedEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()
	
	// Create database
	engine.CreateDatabase("testdb")
	engine.UseDatabase("testdb")
	
	// Create parent table (users)
	usersColumns := []Column{
		{Name: "id", DataType: "INT", PrimaryKey: true},
		{Name: "name", DataType: "VARCHAR"},
	}
	engine.CreateTable("users", usersColumns)
	
	// Insert users
	engine.InsertRow("users", Row{"id": 1, "name": "Alice"})
	engine.InsertRow("users", Row{"id": 2, "name": "Bob"})
	
	// Test FK parsing
	t.Run("Parse FOREIGN KEY", func(t *testing.T) {
		parser := NewParser()
		stmt, err := parser.Parse("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT REFERENCES users(id))")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		
		if len(stmt.Columns) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(stmt.Columns))
		}
		
		userIdCol := stmt.Columns[1]
		if userIdCol.ForeignKey == nil {
			t.Fatal("Expected foreign key on user_id column")
		}
		
		if userIdCol.ForeignKey.RefTable != "users" {
			t.Errorf("Expected RefTable=users, got %s", userIdCol.ForeignKey.RefTable)
		}
		
		if userIdCol.ForeignKey.RefColumn != "id" {
			t.Errorf("Expected RefColumn=id, got %s", userIdCol.ForeignKey.RefColumn)
		}
		
		t.Logf("Foreign key parsed: %+v", userIdCol.ForeignKey)
	})
	
	// Test FK validation
	t.Run("Validate FOREIGN KEY", func(t *testing.T) {
		// Create orders table with FK
		ordersColumns := []Column{
			{Name: "id", DataType: "INT", PrimaryKey: true},
			{Name: "user_id", DataType: "INT", ForeignKey: &ForeignKeyDef{
				RefTable:  "users",
				RefColumn: "id",
				OnDelete:  "RESTRICT",
			}},
			{Name: "amount", DataType: "INT"},
		}
		engine.CreateTable("orders", ordersColumns)
		
		// Valid FK - should succeed
		err := engine.InsertRow("orders", Row{"id": 1, "user_id": 1, "amount": 100})
		if err != nil {
			t.Errorf("Valid FK insert failed: %v", err)
		}
		
		// Invalid FK - should fail
		err = engine.InsertRow("orders", Row{"id": 2, "user_id": 999, "amount": 200})
		if err == nil {
			t.Error("Expected FK violation, but insert succeeded")
		} else {
			t.Logf("Correctly rejected invalid FK: %v", err)
		}
		
		// NULL FK - should succeed (NULL is allowed)
		err = engine.InsertRow("orders", Row{"id": 3, "user_id": nil, "amount": 300})
		if err != nil {
			t.Errorf("NULL FK insert failed: %v", err)
		}
	})
	
	// Test FK with CASCADE
	t.Run("Parse CASCADE", func(t *testing.T) {
		parser := NewParser()
		stmt, err := parser.Parse("CREATE TABLE comments (id INT, post_id INT REFERENCES posts(id) ON DELETE CASCADE)")
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		
		postIdCol := stmt.Columns[1]
		if postIdCol.ForeignKey == nil {
			t.Fatal("Expected foreign key")
		}
		
		if postIdCol.ForeignKey.OnDelete != "CASCADE" {
			t.Errorf("Expected OnDelete=CASCADE, got %s", postIdCol.ForeignKey.OnDelete)
		}
		
		t.Logf("CASCADE parsed: %+v", postIdCol.ForeignKey)
	})
}
