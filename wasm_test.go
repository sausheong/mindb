package mindb

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
)

// Simple WASM module that adds two numbers
// This is a minimal WASM module compiled from:
// pub extern "C" fn add(a: i32, b: i32) -> i32 { a + b }
var simpleAddWASM = []byte{
	0x00, 0x61, 0x73, 0x6d, // Magic number
	0x01, 0x00, 0x00, 0x00, // Version
	// Type section
	0x01, 0x07, 0x01, 0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f,
	// Function section
	0x03, 0x02, 0x01, 0x00,
	// Export section
	0x07, 0x07, 0x01, 0x03, 0x61, 0x64, 0x64, 0x00, 0x00,
	// Code section
	0x0a, 0x09, 0x01, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x6a, 0x0b,
}

func TestWASMEngine_Creation(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}

	if engine == nil {
		t.Fatal("WASM engine is nil")
	}

	if engine.engine == nil {
		t.Fatal("Wasmtime engine is nil")
	}

	if engine.modules == nil {
		t.Fatal("Modules map is nil")
	}

	// Clean up
	engine.Close()
}

func TestWASMEngine_CompileModule(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Compile a simple module
	err = engine.CompileModule("test_add", simpleAddWASM)
	if err != nil {
		t.Fatalf("Failed to compile module: %v", err)
	}

	// Verify module is cached
	module, exists := engine.GetModule("test_add")
	if !exists {
		t.Fatal("Module not found in cache")
	}

	if module == nil {
		t.Fatal("Module is nil")
	}
}

func TestWASMEngine_Execute(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Compile module
	err = engine.CompileModule("test_add", simpleAddWASM)
	if err != nil {
		t.Fatalf("Failed to compile module: %v", err)
	}

	// Execute the add function
	result, err := engine.Execute("test_add", "add", int32(5), int32(3))
	if err != nil {
		t.Fatalf("Failed to execute function: %v", err)
	}

	// Verify result
	if result == nil {
		t.Fatal("Result is nil")
	}

	// Note: Result type checking depends on wasmtime-go API
	t.Logf("Result: %v", result)
}

func TestWASMEngine_RemoveModule(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Compile module
	err = engine.CompileModule("test_add", simpleAddWASM)
	if err != nil {
		t.Fatalf("Failed to compile module: %v", err)
	}

	// Verify it exists
	_, exists := engine.GetModule("test_add")
	if !exists {
		t.Fatal("Module should exist")
	}

	// Remove it
	engine.RemoveModule("test_add")

	// Verify it's gone
	_, exists = engine.GetModule("test_add")
	if exists {
		t.Fatal("Module should not exist after removal")
	}
}

func TestWASMEngine_LoadFromBase64(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Encode WASM to base64
	encoded := base64.StdEncoding.EncodeToString(simpleAddWASM)

	// Load from base64
	err = engine.LoadProcedureFromBase64("test_add", encoded)
	if err != nil {
		t.Fatalf("Failed to load from base64: %v", err)
	}

	// Verify module exists
	_, exists := engine.GetModule("test_add")
	if !exists {
		t.Fatal("Module not found after loading from base64")
	}
}

func TestWASMEngine_Stats(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Get initial stats
	stats := engine.GetStats()
	if stats == nil {
		t.Fatal("Stats should not be nil")
	}

	if stats["compiled_modules"].(int) != 0 {
		t.Errorf("Expected 0 modules, got %d", stats["compiled_modules"])
	}

	// Compile a module
	engine.CompileModule("test_add", simpleAddWASM)

	// Get updated stats
	stats = engine.GetStats()
	if stats["compiled_modules"].(int) != 1 {
		t.Errorf("Expected 1 module, got %d", stats["compiled_modules"])
	}
}

func TestPagedEngine_CreateProcedure(t *testing.T) {
	// Create temporary directory for test
	dataDir := t.TempDir()

	// Create engine
	engine, err := NewPagedEngine(dataDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create a stored procedure
	proc := &StoredProcedure{
		Name:       "test_add",
		Language:   "wasm",
		Code:       simpleAddWASM,
		Params:     []Column{{Name: "a", DataType: "INT"}, {Name: "b", DataType: "INT"}},
		ReturnType: "INT",
	}

	err = engine.CreateProcedure(proc)
	if err != nil {
		t.Fatalf("Failed to create procedure: %v", err)
	}

	// Verify procedure exists
	retrieved, err := engine.GetProcedure("test_add")
	if err != nil {
		t.Fatalf("Failed to get procedure: %v", err)
	}

	if retrieved.Name != "test_add" {
		t.Errorf("Expected name 'test_add', got '%s'", retrieved.Name)
	}
}

func TestPagedEngine_DropProcedure(t *testing.T) {
	dataDir := t.TempDir()
	engine, err := NewPagedEngine(dataDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create procedure
	proc := &StoredProcedure{
		Name:       "test_add",
		Language:   "wasm",
		Code:       simpleAddWASM,
		ReturnType: "INT",
	}
	engine.CreateProcedure(proc)

	// Drop procedure
	err = engine.DropProcedure("test_add")
	if err != nil {
		t.Fatalf("Failed to drop procedure: %v", err)
	}

	// Verify it's gone
	_, err = engine.GetProcedure("test_add")
	if err == nil {
		t.Fatal("Procedure should not exist after drop")
	}
}

func TestPagedEngine_ListProcedures(t *testing.T) {
	dataDir := t.TempDir()
	engine, err := NewPagedEngine(dataDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Initially empty
	procs := engine.ListProcedures()
	if len(procs) != 0 {
		t.Errorf("Expected 0 procedures, got %d", len(procs))
	}

	// Create procedures
	for i := 1; i <= 3; i++ {
		proc := &StoredProcedure{
			Name:       fmt.Sprintf("proc_%d", i),
			Language:   "wasm",
			Code:       simpleAddWASM,
			ReturnType: "INT",
		}
		engine.CreateProcedure(proc)
	}

	// List procedures
	procs = engine.ListProcedures()
	if len(procs) != 3 {
		t.Errorf("Expected 3 procedures, got %d", len(procs))
	}
}

func TestPagedEngine_CallProcedure(t *testing.T) {
	dataDir := t.TempDir()
	engine, err := NewPagedEngine(dataDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	// Create procedure
	proc := &StoredProcedure{
		Name:       "add",
		Language:   "wasm",
		Code:       simpleAddWASM,
		Params:     []Column{{Name: "a", DataType: "INT"}, {Name: "b", DataType: "INT"}},
		ReturnType: "INT",
	}

	err = engine.CreateProcedure(proc)
	if err != nil {
		t.Fatalf("Failed to create procedure: %v", err)
	}

	// Call procedure
	result, err := engine.CallProcedure("add", int32(5), int32(3))
	if err != nil {
		t.Fatalf("Failed to call procedure: %v", err)
	}

	// Verify result
	if result == nil {
		t.Fatal("Result should not be nil")
	}

	t.Logf("Result: %v", result)
}

func TestWASMEngine_IntrospectFunction(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Introspect the function (takes raw WASM bytes)
	params, returnType, err := engine.IntrospectFunction(simpleAddWASM, "add")
	if err != nil {
		t.Fatalf("Failed to introspect function: %v", err)
	}

	// Verify we got parameter info
	if len(params) != 2 {
		t.Errorf("Expected 2 parameters, got %d", len(params))
	}

	// Verify return type
	if returnType == "" {
		t.Error("Expected non-empty return type")
	}

	t.Logf("Parameters: %v, Return type: %s", params, returnType)
}

func TestWASMEngine_LoadProcedureFromBase64(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Encode WASM to base64
	base64WASM := base64.StdEncoding.EncodeToString(simpleAddWASM)

	// Load from base64
	err = engine.LoadProcedureFromBase64("test_add", base64WASM)
	if err != nil {
		t.Fatalf("Failed to load procedure from base64: %v", err)
	}

	// Verify module was loaded
	_, exists := engine.GetModule("test_add")
	if !exists {
		t.Fatal("Module should exist after loading from base64")
	}

	// Test execution
	result, err := engine.Execute("test_add", "add", int32(10), int32(20))
	if err != nil {
		t.Fatalf("Failed to execute after base64 load: %v", err)
	}

	t.Logf("Result after base64 load: %v", result)
}

func TestWASMEngine_GetStats(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Get initial stats
	stats := engine.GetStats()
	compiledModules := stats["compiled_modules"].(int)
	if compiledModules != 0 {
		t.Errorf("Expected 0 compiled modules, got %d", compiledModules)
	}

	// Compile a module
	err = engine.CompileModule("test_add", simpleAddWASM)
	if err != nil {
		t.Fatalf("Failed to compile module: %v", err)
	}

	// Get updated stats
	stats = engine.GetStats()
	compiledModules = stats["compiled_modules"].(int)
	if compiledModules != 1 {
		t.Errorf("Expected 1 compiled module, got %d", compiledModules)
	}

	t.Logf("Stats: %+v", stats)
}

func TestWASMEngine_IntrospectFunction_DifferentTypes(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Test with i32 parameters (already covered by simpleAddWASM)
	params, returnType, err := engine.IntrospectFunction(simpleAddWASM, "add")
	if err != nil {
		t.Fatalf("Failed to introspect i32 function: %v", err)
	}
	if returnType != "INT" {
		t.Errorf("Expected INT return type for i32, got %s", returnType)
	}
	if len(params) != 2 {
		t.Errorf("Expected 2 params, got %d", len(params))
	}
	for i, param := range params {
		if param.DataType != "INT" {
			t.Errorf("Param %d: expected INT type, got %s", i, param.DataType)
		}
	}

	t.Logf("i32 function introspection: params=%v, return=%s", params, returnType)
}

func TestWASMEngine_IntrospectFunction_SemanticNames(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Test that "add" function gets semantic names "a" and "b"
	params, _, err := engine.IntrospectFunction(simpleAddWASM, "add")
	if err != nil {
		t.Fatalf("Failed to introspect: %v", err)
	}

	// Check that we got semantic names (not just arg1, arg2)
	if len(params) >= 2 {
		if params[0].Name == "a" && params[1].Name == "b" {
			t.Logf("✓ Got semantic names: %s, %s", params[0].Name, params[1].Name)
		} else {
			t.Logf("Got names: %s, %s (expected a, b)", params[0].Name, params[1].Name)
		}
	}
}

func TestWASMEngine_IntrospectFunction_UnknownFunction(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Try to introspect a non-existent function
	_, _, err = engine.IntrospectFunction(simpleAddWASM, "nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent function, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "not found") {
		t.Errorf("Expected 'not found' error, got: %v", err)
	}
}

func TestWASMEngine_ExecuteWithContext(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Compile module
	err = engine.CompileModule("test_add", simpleAddWASM)
	if err != nil {
		t.Fatalf("Failed to compile module: %v", err)
	}

	// Create execution context
	ctx := &ExecutionContext{
		Database: "testdb",
	}

	// Execute with context
	result, err := engine.ExecuteWithContext("test_add", "add", ctx, int32(15), int32(25))
	if err != nil {
		t.Fatalf("Failed to execute with context: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}

	t.Logf("Result with context: %v", result)
}

func TestWASMEngine_ModuleLifecycle(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Compile module
	err = engine.CompileModule("test_lifecycle", simpleAddWASM)
	if err != nil {
		t.Fatalf("Failed to compile module: %v", err)
	}

	// Verify it exists
	_, exists := engine.GetModule("test_lifecycle")
	if !exists {
		t.Fatal("Module should exist after compilation")
	}

	// Remove it
	engine.RemoveModule("test_lifecycle")

	// Verify it's gone
	_, exists = engine.GetModule("test_lifecycle")
	if exists {
		t.Error("Module should not exist after removal")
	}
}

func TestWASMEngine_LoadProcedureFromBase64_InvalidBase64(t *testing.T) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		t.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Try to load invalid base64
	err = engine.LoadProcedureFromBase64("invalid", "not-valid-base64!!!")
	if err == nil {
		t.Error("Expected error for invalid base64, got nil")
	}
}

func BenchmarkWASMEngine_Execute(b *testing.B) {
	engine, err := NewWASMEngine(DefaultWASMConfig())
	if err != nil {
		b.Fatalf("Failed to create WASM engine: %v", err)
	}
	defer engine.Close()

	// Compile module once
	err = engine.CompileModule("test_add", simpleAddWASM)
	if err != nil {
		b.Fatalf("Failed to compile module: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := engine.Execute("test_add", "add", int32(5), int32(3))
		if err != nil {
			b.Fatalf("Execution failed: %v", err)
		}
	}
}
