module github.com/sausheong/mindb/cmd/mindb-server

go 1.25.1

require (
	github.com/go-chi/chi/v5 v5.0.11
	github.com/rs/zerolog v1.31.0
	github.com/sausheong/mindb v0.0.0-00010101000000-000000000000
	golang.org/x/net v0.44.0
)

require (
	github.com/bytecodealliance/wasmtime-go/v25 v25.0.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace github.com/sausheong/mindb => ../..
