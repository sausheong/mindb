package mindb

import (
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bytecodealliance/wasmtime-go/v25"
)

// WASMEngine manages WASM stored procedures
type WASMEngine struct {
	engine  *wasmtime.Engine
	modules map[string]*wasmtime.Module // Cached compiled modules
	mu      sync.RWMutex
	config  *WASMConfig
}

// WASMConfig configures the WASM engine
type WASMConfig struct {
	MaxMemoryBytes    uint64        // Maximum memory per instance (default: 100MB)
	MaxExecutionTime  time.Duration // Maximum execution time (default: 5s)
	MaxInstances      int           // Maximum concurrent instances (default: 100)
	EnableFuelMetering bool         // Enable fuel-based execution limits
	FuelLimit         uint64        // Fuel limit per execution
}

// StoredProcedure represents a WASM stored procedure
type StoredProcedure struct {
	Name        string
	Language    string    // "wasm", "rust", "go", etc.
	Code        []byte    // WASM bytecode
	Params      []Column  // Parameter definitions
	ReturnType  string    // Return type (e.g., "INT", "TEXT", "FLOAT")
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// DefaultWASMConfig returns default WASM configuration
func DefaultWASMConfig() *WASMConfig {
	return &WASMConfig{
		MaxMemoryBytes:     100 * 1024 * 1024, // 100MB
		MaxExecutionTime:   5 * time.Second,
		MaxInstances:       100,
		EnableFuelMetering: true,
		FuelLimit:          10000000, // 10 million - increased for complex operations
	}
}

// NewWASMEngine creates a new WASM engine
func NewWASMEngine(config *WASMConfig) (*WASMEngine, error) {
	if config == nil {
		config = DefaultWASMConfig()
	}

	// Create Wasmtime engine with configuration
	engineConfig := wasmtime.NewConfig()
	
	// Enable fuel metering for execution limits
	if config.EnableFuelMetering {
		engineConfig.SetConsumeFuel(true)
	}

	engine := wasmtime.NewEngineWithConfig(engineConfig)

	return &WASMEngine{
		engine:  engine,
		modules: make(map[string]*wasmtime.Module),
		config:  config,
	}, nil
}

// CompileModule compiles WASM bytecode into a module
func (w *WASMEngine) CompileModule(name string, code []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Compile the WASM module
	module, err := wasmtime.NewModule(w.engine, code)
	if err != nil {
		return fmt.Errorf("failed to compile WASM module: %w", err)
	}

	// Cache the compiled module
	w.modules[name] = module

	return nil
}

// IntrospectFunction inspects a WASM function's signature
func (w *WASMEngine) IntrospectFunction(code []byte, functionName string) (params []Column, returnType string, err error) {
	// Compile the module temporarily
	module, err := wasmtime.NewModule(w.engine, code)
	if err != nil {
		return nil, "", fmt.Errorf("failed to compile WASM module: %w", err)
	}

	// Get the function export
	var funcType *wasmtime.FuncType
	for _, exp := range module.Exports() {
		if exp.Name() == functionName {
			if exp.Type().FuncType() != nil {
				funcType = exp.Type().FuncType()
				break
			}
		}
	}

	if funcType == nil {
		return nil, "", fmt.Errorf("function '%s' not found in module", functionName)
	}

	// Extract parameter types
	paramTypes := funcType.Params()
	params = make([]Column, len(paramTypes))
	
	// Generate semantic parameter names based on common patterns
	paramNames := generateParamNames(functionName, len(paramTypes))
	
	for i, pt := range paramTypes {
		paramName := paramNames[i]
		if paramName == "" {
			paramName = fmt.Sprintf("arg%d", i+1)
		}
		params[i] = Column{
			Name:     paramName,
			DataType: wasmTypeToSQL(pt.Kind()),
		}
	}

	// Extract return type
	resultTypes := funcType.Results()
	if len(resultTypes) > 0 {
		returnType = wasmTypeToSQL(resultTypes[0].Kind())
	} else {
		returnType = "VOID"
	}

	return params, returnType, nil
}

// wasmTypeToSQL converts WASM value type to SQL type
func wasmTypeToSQL(kind wasmtime.ValKind) string {
	switch kind {
	case wasmtime.KindI32:
		return "INT"
	case wasmtime.KindI64:
		return "BIGINT"
	case wasmtime.KindF32:
		return "FLOAT"
	case wasmtime.KindF64:
		return "FLOAT"
	default:
		return "INT" // Default fallback
	}
}

// generateParamNames generates semantic parameter names based on function name patterns
func generateParamNames(functionName string, paramCount int) []string {
	names := make([]string, paramCount)
	
	// Common patterns for specific function types
	patterns := map[string][]string{
		"calculate_discount":      {"price", "tier"},
		"calculate_bulk_discount": {"price", "quantity"},
		"calculate_loyalty_points": {"amount", "is_premium"},
		"apply_discount":          {"amount", "discount_rate"},
		"get_price":               {"item_id", "quantity"},
		"validate":                {"value", "threshold"},
		"compare":                 {"a", "b"},
		"add":                     {"a", "b"},
		"subtract":                {"a", "b"},
		"multiply":                {"a", "b"},
		"divide":                  {"numerator", "denominator"},
	}
	
	// Check if we have a pattern for this function
	if pattern, exists := patterns[functionName]; exists {
		for i := 0; i < paramCount && i < len(pattern); i++ {
			names[i] = pattern[i]
		}
		return names
	}
	
	// Generic naming based on common prefixes
	if strings.HasPrefix(functionName, "calculate_") {
		if paramCount >= 1 {
			names[0] = "value"
		}
		if paramCount >= 2 {
			names[1] = "factor"
		}
	} else if strings.HasPrefix(functionName, "get_") || strings.HasPrefix(functionName, "fetch_") {
		if paramCount >= 1 {
			names[0] = "id"
		}
	} else if strings.HasPrefix(functionName, "validate_") || strings.HasPrefix(functionName, "check_") {
		if paramCount >= 1 {
			names[0] = "value"
		}
		if paramCount >= 2 {
			names[1] = "criteria"
		}
	}
	
	// Fill remaining with generic names
	for i := 0; i < paramCount; i++ {
		if names[i] == "" {
			names[i] = fmt.Sprintf("arg%d", i+1)
		}
	}
	
	return names
}

// GetModule retrieves a compiled module
func (w *WASMEngine) GetModule(name string) (*wasmtime.Module, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	module, exists := w.modules[name]
	return module, exists
}

// RemoveModule removes a compiled module from cache
func (w *WASMEngine) RemoveModule(name string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.modules, name)
}

// Execute executes a stored procedure
func (w *WASMEngine) Execute(procName string, functionName string, args ...interface{}) (interface{}, error) {
	// Get compiled module
	module, exists := w.GetModule(procName)
	if !exists {
		return nil, fmt.Errorf("stored procedure '%s' not found", procName)
	}

	// Create a new store for this execution
	store := wasmtime.NewStore(w.engine)

	// Set fuel limit if enabled
	if w.config.EnableFuelMetering {
		store.SetFuel(w.config.FuelLimit)
	}

	// Create linker for host functions
	linker := wasmtime.NewLinker(w.engine)

	// TODO: Add host functions here (database access, etc.)

	// Instantiate the module
	instance, err := linker.Instantiate(store, module)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate module: %w", err)
	}

	// Get the exported function
	fn := instance.GetFunc(store, functionName)
	if fn == nil {
		return nil, fmt.Errorf("function '%s' not found in module", functionName)
	}

	// Convert Go args to WASM values
	wasmArgs := make([]interface{}, len(args))
	copy(wasmArgs, args)

	// Execute with timeout
	resultChan := make(chan interface{}, 1)
	errorChan := make(chan error, 1)

	go func() {
		result, err := fn.Call(store, wasmArgs...)
		if err != nil {
			errorChan <- err
			return
		}
		resultChan <- result
	}()

	// Wait for result or timeout
	select {
	case result := <-resultChan:
		return result, nil
	case err := <-errorChan:
		return nil, fmt.Errorf("execution error: %w", err)
	case <-time.After(w.config.MaxExecutionTime):
		return nil, fmt.Errorf("execution timeout after %v", w.config.MaxExecutionTime)
	}
}

// ExecuteWithContext executes a stored procedure with additional context
func (w *WASMEngine) ExecuteWithContext(procName string, functionName string, ctx *ExecutionContext, args ...interface{}) (interface{}, error) {
	// Get compiled module
	module, exists := w.GetModule(procName)
	if !exists {
		return nil, fmt.Errorf("stored procedure '%s' not found", procName)
	}

	// Create a new store for this execution
	store := wasmtime.NewStore(w.engine)

	// Set fuel limit if enabled
	if w.config.EnableFuelMetering {
		store.SetFuel(w.config.FuelLimit)
	}

	// Create linker with host functions
	linker := wasmtime.NewLinker(w.engine)

	// Add host functions for database access
	if ctx != nil && ctx.Engine != nil {
		w.addHostFunctions(linker, store, ctx)
	}

	// Instantiate the module
	instance, err := linker.Instantiate(store, module)
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate module: %w", err)
	}

	// Get the exported function
	fn := instance.GetFunc(store, functionName)
	if fn == nil {
		return nil, fmt.Errorf("function '%s' not found in module", functionName)
	}

	// Convert Go args to WASM values
	wasmArgs := make([]interface{}, len(args))
	copy(wasmArgs, args)

	// Execute with timeout
	resultChan := make(chan interface{}, 1)
	errorChan := make(chan error, 1)

	go func() {
		result, err := fn.Call(store, wasmArgs...)
		if err != nil {
			errorChan <- err
			return
		}
		resultChan <- result
	}()

	// Wait for result or timeout
	select {
	case result := <-resultChan:
		return result, nil
	case err := <-errorChan:
		return nil, fmt.Errorf("execution error: %w", err)
	case <-time.After(w.config.MaxExecutionTime):
		return nil, fmt.Errorf("execution timeout after %v", w.config.MaxExecutionTime)
	}
}

// ExecutionContext provides context for WASM execution
type ExecutionContext struct {
	Engine      *PagedEngine
	Database    string
	Transaction *Transaction
	UserID      string
}

// addHostFunctions adds host functions that WASM can call
func (w *WASMEngine) addHostFunctions(linker *wasmtime.Linker, store wasmtime.Storelike, ctx *ExecutionContext) {
	// Example: Add a simple logging function
	// linker.FuncWrap("env", "log", func(caller *wasmtime.Caller, ptr int32, len int32) {
	//     // Read string from WASM memory and log it
	//     // Implementation depends on memory access patterns
	// })

	// TODO: Add more host functions:
	// - mindb_query: Execute SQL query
	// - mindb_get_row: Get single row
	// - mindb_insert: Insert row
	// - mindb_update: Update rows
	// - mindb_delete: Delete rows
}

// LoadProcedureFromBase64 loads a procedure from base64-encoded WASM
func (w *WASMEngine) LoadProcedureFromBase64(name string, base64Code string) error {
	code, err := base64.StdEncoding.DecodeString(base64Code)
	if err != nil {
		return fmt.Errorf("failed to decode base64: %w", err)
	}

	return w.CompileModule(name, code)
}

// GetStats returns statistics about the WASM engine
func (w *WASMEngine) GetStats() map[string]interface{} {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return map[string]interface{}{
		"compiled_modules":   len(w.modules),
		"max_memory_bytes":   w.config.MaxMemoryBytes,
		"max_execution_time": w.config.MaxExecutionTime.String(),
		"fuel_enabled":       w.config.EnableFuelMetering,
		"fuel_limit":         w.config.FuelLimit,
	}
}

// Close cleans up the WASM engine
func (w *WASMEngine) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Clear all modules
	w.modules = make(map[string]*wasmtime.Module)

	return nil
}
