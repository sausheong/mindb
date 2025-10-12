# WASM Stored Procedures - Complete Guide

## Overview

Mindb supports WebAssembly (WASM) stored procedures, enabling high-performance, sandboxed logic written in any language that compiles to WASM (Rust, TinyGo, C/C++, AssemblyScript, etc.). Procedures are uploaded over HTTP as base64-encoded WASM modules, stored by name, and invoked via API calls. Execution is sandboxed with memory and time limits.

Status: Production Ready
Performance: 1–2ms/call (cached)
Security: No FS/Network/System calls; memory/time limits

---

## Quick Start

1) Compile to WASM and encode
```bash
# Rust
rustc --target wasm32-unknown-unknown -O code.rs -o code.wasm
base64 -i code.wasm -o code.b64

# TinyGo
tinygo build -o code.wasm -target wasi code.go
base64 -i code.wasm -o code.b64
```

2) Create the procedure
```bash
curl -XPOST http://localhost:8080/procedures \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my_proc",
    "language": "wasm",
    "wasm_base64": "'"'"$(cat code.b64)"'"'",
    "params": [{"name": "x", "data_type": "INT"}],
    "return_type": "INT"
  }'
```

3) Call it
```bash
curl -XPOST http://localhost:8080/procedures/my_proc/call \
  -H "Content-Type: application/json" \
  -d '{"args": [42]}'
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/procedures` | Create a stored procedure |
| DELETE | `/procedures/{name}` | Drop a stored procedure |
| GET | `/procedures` | List stored procedures |
| POST | `/procedures/{name}/call` | Execute a procedure |

Headers
- Content-Type: application/json (POSTs)
- X-Mindb-Database: mydb (optional)

### Create
Request
```json
{
  "name": "calculate_discount",
  "language": "wasm",
  "wasm_base64": "AGFzbQEAAA...",
  "params": [
    {"name": "price", "data_type": "FLOAT"},
    {"name": "tier", "data_type": "INT"}
  ],
  "return_type": "FLOAT",
  "description": "Calculate discount based on tier"
}
```
Response
```json
{"name":"calculate_discount","message":"Procedure created successfully","latency_ms":5}
```

### Call
Request
```json
{"args": [100.0, 2]}
```
Response
```json
{"result": 90.0, "latency_ms": 2}
```

### List
Response
```json
{
  "procedures": [
    {
      "name": "calculate_discount",
      "language": "wasm",
      "params": [
        {"name":"price","data_type":"FLOAT"},
        {"name":"tier","data_type":"INT"}
      ],
      "return_type": "FLOAT",
      "description": "Calculate discount based on customer tier",
      "created_at": "2025-10-05T16:00:00Z",
      "updated_at": "2025-10-05T16:00:00Z"
    }
  ],
  "count": 1,
  "latency_ms": 1
}
```

### Drop
Response
```json
{"name":"calculate_discount","message":"Procedure dropped successfully","latency_ms":2}
```

---

## Code Templates

Rust
```rust
#[no_mangle]
pub extern "C" fn my_function(x: i32) -> i32 {
    x * 2
}
```

TinyGo
```go
//export my_function
func my_function(x int32) int32 {
    return x * 2
}

func main() {}
```

---

## End-to-End Workflow Example (Rust)

1) Write code
```rust
// discount.rs
#[no_mangle]
pub extern "C" fn calculate_discount(price: f64, tier: i32) -> f64 {
    match tier {
        1 => price * 0.95,
        2 => price * 0.90,
        3 => price * 0.85,
        _ => price,
    }
}
```

2) Compile and encode
```bash
rustc --target wasm32-unknown-unknown -O discount.rs -o discount.wasm
base64 -i discount.wasm -o discount.b64
```

3) Upload
```bash
curl -XPOST http://localhost:8080/procedures \
  -H "Content-Type: application/json" \
  -d '{
    "name": "calculate_discount",
    "language": "wasm",
    "wasm_base64": "'"'"$(cat discount.b64)"'"'",
    "params": [
      {"name": "price", "data_type": "FLOAT"},
      {"name": "tier", "data_type": "INT"}
    ],
    "return_type": "FLOAT"
  }'
```

4) Call
```bash
curl -XPOST http://localhost:8080/procedures/calculate_discount/call \
  -H "Content-Type: application/json" \
  -d '{"args": [100.0, 2]}'
```

---

## Performance

- Create: 5–10ms (includes compilation)
- Call (first): 2–5ms (instance creation + execution)
- Call (cached): 1–2ms
- List: <1ms
- Drop: 1–2ms

Tips
- Compile with optimizations (`-O`, `-O3`, TinyGo `-opt=2`).
- Keep functions small and focused; avoid long loops.
- Reuse procedures—modules are compiled once and cached.

---

## Security

All procedures run in a WASM sandbox:
- No file system or network access
- No system calls
- Memory limit: 100MB (default)
- Timeout: 5s (default)

Best practices
- Review code before upload
- Test with small inputs first
- Monitor execution times and error logs

---

## Troubleshooting

- Failed to compile WASM: validate the module (`wasmtime validate`), recheck compiler flags and exported function.
- Procedure not found: confirm the name and that creation succeeded.
- Type mismatch: ensure argument types align with parameter definitions and return type.
- Execution timeout: optimize code or split work into smaller procedures.

---

## Implementation Summary (for maintainers)

HTTP API
```go
// Routes in main.go
r.Post("/procedures", handlers.CreateProcedureHandler())
r.Delete("/procedures/{name}", handlers.DropProcedureHandler())
r.Get("/procedures", handlers.ListProceduresHandler())
r.Post("/procedures/{name}/call", handlers.CallProcedureHandler())
```

Key components
- `internal/api/types.go`: request/response types (create/drop/list/call, parameters, return types)
- `internal/api/handlers.go`: handlers for create/drop/list/call
- `internal/db/adapter.go`: decoding base64, bridging to engine
- `engine_adapter.go`: adapter to `PagedEngine`
- `WASMEngine` (backed by wasmtime): compile/cache/execute with limits

Architecture flow
```
Client → /procedures → handlers → db.Adapter → EngineAdapter → PagedEngine → WASMEngine/wasmtime → Response
```

Monitoring
- `GET /procedures` for inventory
- `GET /health` to verify server status
- Logs include create/call/drop with latencies

---

## Appendix: Request/Response Examples

Create (JSON)
```json
{
  "name": "validate_credit_card",
  "language": "wasm",
  "wasm_base64": "AGFzbQEAAA...",
  "params": [{"name": "number", "data_type": "INT"}],
  "return_type": "INT"
}
```

Call (JSON)
```json
{"args": [4532015112830366]}
```

Success
```json
{"result": 1, "latency_ms": 1}
```

Error
```json
{"error": {"code": "INTERNAL_ERROR", "message": "procedure not found"}}
```
