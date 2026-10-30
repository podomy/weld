module github.com/podomy/weld/cmd-weld

go 1.26.3

require github.com/podomy/weld v0.0.0

// Separate module on purpose: cmd/weld stays outside the parent go.work
// (root plus sdk) so the CLI builds without pulling testbed or SDK graphs.
// The replace pins local development to this working tree. Keep the module
// path as is, do not merge into the parent, and build with GOWORK=off.
replace github.com/podomy/weld => ../..
