// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Command weld parameter verbs backed by the config store.
//
// This file implements the params verbs declare, get, set, describe,
// list, events, dump, and load. The verbs operate on the transport side
// view of the core config store (dds.MemoryStore). The same store backs
// the standard ROS parameter services on the wire in
// internal/transports/dds/params.go, so CLI behavior matches ros2cli
// behavior: declare fails when already present, set validates ranges
// and read only flags, atomic set applies all or none, and events
// record every applied change with the owning node name.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/podomy/weld/internal/transports/dds"
)

// Default node for params verbs when no flag is given.
const defaultParamsNode = "/weld"

// validateParamsNode rejects empty names and names not starting with slash.
func validateParamsNode(node string) error {
	if node == "" {
		return errors.New("weld: node name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if node[0] != '/' {
		return fmt.Errorf("weld: node %q must start with slash", node)
	}
	return nil
}

// paramsTypeName returns the lowercase type name for a type code.
func paramsTypeName(t uint8) string {
	switch t {
	case dds.ParamNotSet:
		return "not_set"
	case dds.ParamBool:
		return "bool"
	case dds.ParamInteger:
		return "integer"
	case dds.ParamDouble:
		return "double"
	case dds.ParamString:
		return "string"
	case dds.ParamByteArray:
		return "byte_array"
	case dds.ParamBoolArray:
		return "bool_array"
	case dds.ParamIntegerArray:
		return "integer_array"
	case dds.ParamDoubleArray:
		return "double_array"
	case dds.ParamStringArray:
		return "string_array"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

// parseParamsType maps a type name to its code.
func parseParamsType(s string) (uint8, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "not_set", "notset", "unset":
		return dds.ParamNotSet, nil
	case "bool", "boolean":
		return dds.ParamBool, nil
	case "int", "integer", "int64":
		return dds.ParamInteger, nil
	case "double", "float", "float64":
		return dds.ParamDouble, nil
	case "string", "str":
		return dds.ParamString, nil
	case "byte_array", "byte-array", "bytes", "octet":
		return dds.ParamByteArray, nil
	case "bool_array", "bool-array":
		return dds.ParamBoolArray, nil
	case "integer_array", "integer-array", "int_array":
		return dds.ParamIntegerArray, nil
	case "double_array", "double-array", "float_array":
		return dds.ParamDoubleArray, nil
	case "string_array", "string-array":
		return dds.ParamStringArray, nil
	default:
		return 0, fmt.Errorf("weld: unknown parameter type %q", s)
	}
}

// splitCSV splits comma separated values and trims spaces.
func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, strings.TrimSpace(p))
	}
	return out
}

// parseParamsBool parses one bool value.
func parseParamsBool(s string) (bool, error) {
	trimmed := strings.ToLower(strings.TrimSpace(s))
	switch trimmed {
	case "true", "1":
		return true, nil
	case "false", "0":
		return false, nil
	default:
		return false, fmt.Errorf("weld: invalid bool %q", s)
	}
}

// parseParamsValue parses text into a typed ParamValue.
// parseParamsScalar parses scalar text into a typed ParamValue.
func parseParamsScalar(typeCode uint8, text string) (dds.ParamValue, error) {
	var out dds.ParamValue
	out.Type = typeCode
	switch typeCode {
	case dds.ParamNotSet:
		return dds.ParamValue{Type: dds.ParamNotSet}, nil
	case dds.ParamBool:
		b, err := parseParamsBool(text)
		if err != nil {
			return out, err
		}
		out.Bool = b
		return out, nil
	case dds.ParamInteger:
		v, err := strconv.ParseInt(strings.TrimSpace(text), 10, 64)
		if err != nil {
			return out, fmt.Errorf("weld: invalid integer %q: %w", text, err)
		}
		out.Integer = v
		return out, nil
	case dds.ParamDouble:
		v, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
		if err != nil {
			return out, fmt.Errorf("weld: invalid double %q: %w", text, err)
		}
		out.Double = v
		return out, nil
	case dds.ParamString:
		out.String = text
		return out, nil
	default:
		return out, fmt.Errorf("weld: unknown parameter type %d", typeCode)
	}
}

// parseParamsByteArray parses byte array text.
func parseParamsByteArray(text string) ([]byte, error) {
	parts := splitCSV(text)
	byteArray := make([]byte, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}
		v, err := strconv.ParseUint(p, 10, 8)
		if err != nil {
			return nil, fmt.Errorf("weld: invalid byte %q: %w", p, err)
		}
		byteArray = append(byteArray, byte(v))
	}
	return byteArray, nil
}

// parseParamsBoolArray parses bool array text.
func parseParamsBoolArray(text string) ([]bool, error) {
	parts := splitCSV(text)
	boolArray := make([]bool, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}
		b, err := parseParamsBool(p)
		if err != nil {
			return nil, err
		}
		boolArray = append(boolArray, b)
	}
	return boolArray, nil
}

// parseParamsIntegerArray parses integer array text.
func parseParamsIntegerArray(text string) ([]int64, error) {
	parts := splitCSV(text)
	intArray := make([]int64, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}
		v, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("weld: invalid integer %q: %w", p, err)
		}
		intArray = append(intArray, v)
	}
	return intArray, nil
}

// parseParamsDoubleArray parses double array text.
func parseParamsDoubleArray(text string) ([]float64, error) {
	parts := splitCSV(text)
	doubleArray := make([]float64, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}
		v, err := strconv.ParseFloat(p, 64)
		if err != nil {
			return nil, fmt.Errorf("weld: invalid double %q: %w", p, err)
		}
		doubleArray = append(doubleArray, v)
	}
	return doubleArray, nil
}

// parseParamsArray parses array text into a typed ParamValue.
func parseParamsArray(typeCode uint8, text string) (dds.ParamValue, error) {
	var out dds.ParamValue
	out.Type = typeCode
	switch typeCode {
	case dds.ParamByteArray:
		byteArray, err := parseParamsByteArray(text)
		if err != nil {
			return out, err
		}
		out.ByteArray = byteArray
		return out, nil
	case dds.ParamBoolArray:
		boolArray, err := parseParamsBoolArray(text)
		if err != nil {
			return out, err
		}
		out.BoolArray = boolArray
		return out, nil
	case dds.ParamIntegerArray:
		intArray, err := parseParamsIntegerArray(text)
		if err != nil {
			return out, err
		}
		out.IntegerArray = intArray
		return out, nil
	case dds.ParamDoubleArray:
		doubleArray, err := parseParamsDoubleArray(text)
		if err != nil {
			return out, err
		}
		out.DoubleArray = doubleArray
		return out, nil
	case dds.ParamStringArray:
		parts := splitCSV(text)
		out.StringArray = parts
		return out, nil
	default:
		return out, fmt.Errorf("weld: unknown parameter type %d", typeCode)
	}
}

// parseParamsValue parses text into a typed ParamValue.
func parseParamsValue(typeCode uint8, text string) (dds.ParamValue, error) {
	switch typeCode {
	case dds.ParamNotSet, dds.ParamBool, dds.ParamInteger, dds.ParamDouble, dds.ParamString:
		return parseParamsScalar(typeCode, text)
	case dds.ParamByteArray, dds.ParamBoolArray, dds.ParamIntegerArray, dds.ParamDoubleArray, dds.ParamStringArray:
		return parseParamsArray(typeCode, text)
	default:
		return dds.ParamValue{Type: typeCode}, fmt.Errorf("weld: unknown parameter type %d", typeCode)
	}
}

// formatParamsScalar renders scalar param values for CLI output.
func formatParamsScalar(value dds.ParamValue) string {
	switch value.Type {
	case dds.ParamNotSet:
		return "not_set"
	case dds.ParamBool:
		return strconv.FormatBool(value.Bool)
	case dds.ParamInteger:
		return strconv.FormatInt(value.Integer, 10)
	case dds.ParamDouble:
		return strconv.FormatFloat(value.Double, 'g', -1, 64)
	case dds.ParamString:
		return value.String
	default:
		return ""
	}
}

// formatParamsArray renders array param values for CLI output.
func formatParamsArray(value dds.ParamValue) string {
	switch value.Type {
	case dds.ParamByteArray:
		parts := make([]string, 0, len(value.ByteArray))
		for _, b := range value.ByteArray {
			parts = append(parts, strconv.Itoa(int(b)))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case dds.ParamBoolArray:
		parts := make([]string, 0, len(value.BoolArray))
		for _, b := range value.BoolArray {
			parts = append(parts, strconv.FormatBool(b))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case dds.ParamIntegerArray:
		parts := make([]string, 0, len(value.IntegerArray))
		for _, v := range value.IntegerArray {
			parts = append(parts, strconv.FormatInt(v, 10))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case dds.ParamDoubleArray:
		parts := make([]string, 0, len(value.DoubleArray))
		for _, v := range value.DoubleArray {
			parts = append(parts, strconv.FormatFloat(v, 'g', -1, 64))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case dds.ParamStringArray:
		return "[" + strings.Join(value.StringArray, ", ") + "]"
	default:
		return ""
	}
}

// formatParamsValue renders one value for CLI output.
func formatParamsValue(value dds.ParamValue) string {
	switch value.Type {
	case dds.ParamNotSet, dds.ParamBool, dds.ParamInteger, dds.ParamDouble, dds.ParamString:
		return formatParamsScalar(value)
	case dds.ParamByteArray, dds.ParamBoolArray, dds.ParamIntegerArray, dds.ParamDoubleArray, dds.ParamStringArray:
		return formatParamsArray(value)
	default:
		return fmt.Sprintf("unknown(%d)", value.Type)
	}
}

// ensureParamsStore returns a node-scoped store for the requested node.
// A nil store or a store owned by a different node yields a fresh store
// for node so --node selects the operating store instead of always using
// /weld. Callers pass the shared store; node selection is per invocation.
func ensureParamsStore(store *dds.MemoryStore, node string) *dds.MemoryStore {
	if store != nil && store.NodeName() == node {
		return store
	}
	return dds.NewMemoryStore(node)
}

// printParamsUsage writes params help.
func printParamsUsage(w io.Writer) {
	_, _ = fmt.Fprint(w, "Usage: weld params <declare|get|set|describe|list|events|dump|load> [flags]\n\ndeclare: add one parameter, fails when already present\n  weld params declare my_param 42 --type integer --node /weld\n\nget: print one or more values\n  weld params get my_param other --node /weld\n\nset: set one parameter\n  weld params set my_param 43 --type integer --node /weld\n  weld params set my_param 1,2,3 --type integer_array --atomic --node /weld\n\ndescribe: print descriptors\n  weld params describe my_param --node /weld\n\nlist: print names with optional prefixes, depth, and types\n  weld params list --prefixes ctrl --depth 0 --types --node /weld\n\nevents: print recorded parameter events\n  weld params events --node /weld\n\ndump: write canonical YAML to stdout or file\n  weld params dump --file params.yaml --node /weld\n\nload: read canonical YAML from file or stdin\n  weld params load --file params.yaml --node /weld\n\nFlags:\n  --node string   owning node name (default /weld)\n  --type string   value type: bool, integer, double, string, byte_array, bool_array, integer_array, double_array, string_array\n  --prefixes string comma separated list prefixes for list\n  --depth uint    list depth, 0 means recursive (default 0)\n  --types         list prints types alongside names\n  --atomic        set applies atomically\n  --file string   dump or load file, empty means stdout or stdin\n") //nolint:errcheck // help output failure is not actionable
}

// dispatchParamsVerb runs one params verb, reporting whether handled.
func dispatchParamsVerb(verb string, rest []string, stdout, stderr io.Writer, store *dds.MemoryStore) (int, bool) {
	switch verb {
	case "declare":
		return runParamsDeclareCommand(rest, stdout, stderr, store), true
	case "get":
		return runParamsGetCommand(rest, stdout, stderr, store), true
	case "set":
		return runParamsSetCommand(rest, stdout, stderr, store), true
	case "describe":
		return runParamsDescribeCommand(rest, stdout, stderr, store), true
	case "list":
		return runParamsListCommand(rest, stdout, stderr, store), true
	case "events":
		return runParamsEventsCommand(rest, stdout, stderr, store), true
	case "dump":
		return runParamsDumpCommand(rest, stdout, stderr, store), true
	case "load":
		return runParamsLoadCommand(rest, stdout, stderr, store), true
	case "-h", "-help", "--help", "help":
		printParamsUsage(stdout)
		return 0, true
	default:
		return 0, false
	}
}

// runParamsCommand dispatches params verbs and maps errors to exit codes.
func runParamsCommand(ctx context.Context, args []string, stdout, stderr io.Writer, store *dds.MemoryStore) int {
	err := ctx.Err()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}
	if len(args) == 0 {
		printParamsUsage(stderr)
		return 2
	}
	if isHelpRequest(args) {
		printParamsUsage(stdout)
		return 0
	}
	verb := args[0]
	rest := args[1:]
	if code, ok := dispatchParamsVerb(verb, rest, stdout, stderr, store); ok {
		return code
	}
	_, _ = fmt.Fprintf(stderr, "weld: unknown params subcommand %q\n", verb) //nolint:errcheck // final error output is best effort
	return 2
}

// paramsNodeFlag registers the node flag and returns its pointer.
func paramsNodeFlag(fs *flag.FlagSet) *string {
	return fs.String("node", defaultParamsNode, "owning node name such as /weld")
}

// resolveParamsStore resolves node and returns a node-scoped owned store.
func resolveParamsStore(nodeFlag *string, store *dds.MemoryStore, stderr io.Writer) (*dds.MemoryStore, bool) {
	node := *nodeFlag
	err := validateParamsNode(node)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return nil, false
	}
	return ensureParamsStore(store, node), true
}

// runParamsDeclareCommand adds one parameter.
func runParamsDeclareCommand(args []string, stdout, stderr io.Writer, store *dds.MemoryStore) int {
	fs := flag.NewFlagSet("params declare", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if isHelpRequest(args) {
		printParamsUsage(stdout)
		return 0
	}
	nodeFlag := paramsNodeFlag(fs)
	typeFlag := fs.String("type", "string", "value type")
	descFlag := fs.String("description", "", "descriptor description")
	readOnlyFlag := fs.Bool("read-only", false, "mark descriptor read only")
	dynamicFlag := fs.Bool("dynamic-typing", false, "allow type changes")
	err := fs.Parse(args)
	if err != nil {
		return 2
	}
	pos := fs.Args()
	if len(pos) != 2 {
		_, _ = fmt.Fprintln(stderr, "weld: params declare needs <name> <value>") //nolint:errcheck // final error output is best effort
		return 2
	}
	name := pos[0]
	text := pos[1]
	typeCode, err := parseParamsType(*typeFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 2
	}
	value, err := parseParamsValue(typeCode, text)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 2
	}
	owned, ok := resolveParamsStore(nodeFlag, store, stderr)
	if !ok {
		return 2
	}
	desc := dds.ParamDescriptor{
		Name:          name,
		Type:          typeCode,
		Description:   *descFlag,
		ReadOnly:      *readOnlyFlag,
		DynamicTyping: *dynamicFlag,
	}
	err = owned.Declare(dds.Parameter{Name: name, Value: value}, desc)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "weld: declare "+oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}
	_, err = fmt.Fprintf(stdout, "%s=%s (%s)\n", name, formatParamsValue(value), paramsTypeName(typeCode))
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}
	return 0
}

// runParamsGetCommand prints one or more values.
func runParamsGetCommand(args []string, stdout, stderr io.Writer, store *dds.MemoryStore) int {
	fs := flag.NewFlagSet("params get", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if isHelpRequest(args) {
		printParamsUsage(stdout)
		return 0
	}
	nodeFlag := paramsNodeFlag(fs)
	err := fs.Parse(args)
	if err != nil {
		return 2
	}
	names := fs.Args()
	if len(names) == 0 {
		_, _ = fmt.Fprintln(stderr, "weld: params get needs at least one <name>") //nolint:errcheck // final error output is best effort
		return 2
	}
	owned, ok := resolveParamsStore(nodeFlag, store, stderr)
	if !ok {
		return 2
	}
	failed := false
	for _, n := range names {
		value, present := owned.Get(n)
		if !present {
			_, _ = fmt.Fprintf(stderr, "weld: parameter %q is not set\n", n) //nolint:errcheck // final error output is best effort
			failed = true
			continue
		}
		_, err = fmt.Fprintf(stdout, "%s=%s (%s)\n", n, formatParamsValue(value), paramsTypeName(value.Type))
		if err != nil {
			_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
			return 1
		}
	}
	if failed {
		return 1
	}
	return 0
}

// applyParamsSetAtomic applies one param atomically, reporting failures.
func applyParamsSetAtomic(owned *dds.MemoryStore, param dds.Parameter, stderr io.Writer) int {
	result := owned.SetAtomically([]dds.Parameter{param})
	if result.Successful {
		return 0
	}
	_, _ = fmt.Fprintln(stderr, oneLine(errors.New(result.Reason))) //nolint:errcheck // reason is already weld style text
	if result.Reason == "" {
		_, _ = fmt.Fprintf(stderr, "weld: atomic set of %q failed\n", param.Name) //nolint:errcheck // final error output is best effort
	}
	return 1
}

// applyParamsSetMany applies one param non-atomically, reporting failures.
func applyParamsSetMany(owned *dds.MemoryStore, param dds.Parameter, stderr io.Writer) int {
	results := owned.SetMany([]dds.Parameter{param})
	if len(results) == 1 && results[0].Successful {
		return 0
	}
	reason := "weld: set failed"
	if len(results) == 1 && results[0].Reason != "" {
		reason = results[0].Reason
	}
	_, _ = fmt.Fprintln(stderr, oneLine(errors.New(reason))) //nolint:errcheck // reason carries store context already
	return 1
}

// runParamsSetCommand sets one parameter with optional atomic flag.
func runParamsSetCommand(args []string, stdout, stderr io.Writer, store *dds.MemoryStore) int {
	fs := flag.NewFlagSet("params set", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if isHelpRequest(args) {
		printParamsUsage(stdout)
		return 0
	}
	nodeFlag := paramsNodeFlag(fs)
	typeFlag := fs.String("type", "string", "value type")
	atomicFlag := fs.Bool("atomic", false, "apply atomically")
	err := fs.Parse(args)
	if err != nil {
		return 2
	}
	pos := fs.Args()
	if len(pos) != 2 {
		_, _ = fmt.Fprintln(stderr, "weld: params set needs <name> <value>") //nolint:errcheck // final error output is best effort
		return 2
	}
	name := pos[0]
	text := pos[1]
	typeCode, err := parseParamsType(*typeFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 2
	}
	value, err := parseParamsValue(typeCode, text)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 2
	}
	owned, ok := resolveParamsStore(nodeFlag, store, stderr)
	if !ok {
		return 2
	}
	param := dds.Parameter{Name: name, Value: value}
	if *atomicFlag {
		code := applyParamsSetAtomic(owned, param, stderr)
		if code != 0 {
			return code
		}
	} else {
		code := applyParamsSetMany(owned, param, stderr)
		if code != 0 {
			return code
		}
	}
	_, err = fmt.Fprintf(stdout, "%s=%s (%s)\n", name, formatParamsValue(value), paramsTypeName(typeCode))
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}
	return 0
}

// runParamsDescribeCommand prints descriptors for names.
func runParamsDescribeCommand(args []string, stdout, stderr io.Writer, store *dds.MemoryStore) int {
	fs := flag.NewFlagSet("params describe", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if isHelpRequest(args) {
		printParamsUsage(stdout)
		return 0
	}
	nodeFlag := paramsNodeFlag(fs)
	err := fs.Parse(args)
	if err != nil {
		return 2
	}
	names := fs.Args()
	if len(names) == 0 {
		_, _ = fmt.Fprintln(stderr, "weld: params describe needs at least one <name>") //nolint:errcheck // final error output is best effort
		return 2
	}
	owned, ok := resolveParamsStore(nodeFlag, store, stderr)
	if !ok {
		return 2
	}
	descs := owned.Describe(names)
	for _, d := range descs {
		_, err = fmt.Fprintf(stdout, "%s: type=%s read_only=%v dynamic_typing=%v description=%q\n", d.Name, paramsTypeName(d.Type), d.ReadOnly, d.DynamicTyping, d.Description)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
			return 1
		}
	}
	return 0
}

// parseListPrefixes splits comma separated prefixes, dropping blanks.
func parseListPrefixes(prefixesFlag string) []string {
	if strings.TrimSpace(prefixesFlag) == "" {
		return nil
	}
	var prefixes []string
	for p := range strings.SplitSeq(prefixesFlag, ",") {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		prefixes = append(prefixes, trimmed)
	}
	return prefixes
}

// printParamsListNames prints list names with optional types.
func printParamsListNames(stdout, stderr io.Writer, owned *dds.MemoryStore, names []string, withTypes bool) int {
	for _, n := range names {
		if !withTypes {
			_, err := fmt.Fprintln(stdout, n)
			if err != nil {
				_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
				return 1
			}
			continue
		}
		value, present := owned.Get(n)
		typeName := paramsTypeName(dds.ParamNotSet)
		if present {
			typeName = paramsTypeName(value.Type)
		}
		_, err := fmt.Fprintf(stdout, "%s (%s)\n", n, typeName)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
			return 1
		}
	}
	return 0
}

// runParamsListCommand prints names with optional types.
func runParamsListCommand(args []string, stdout, stderr io.Writer, store *dds.MemoryStore) int {
	fs := flag.NewFlagSet("params list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if isHelpRequest(args) {
		printParamsUsage(stdout)
		return 0
	}
	nodeFlag := paramsNodeFlag(fs)
	prefixesFlag := fs.String("prefixes", "", "comma separated prefixes")
	depthFlag := fs.Uint64("depth", dds.ParamDepthRecursive, "list depth, 0 means recursive")
	typesFlag := fs.Bool("types", false, "print types alongside names")
	err := fs.Parse(args)
	if err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		_, _ = fmt.Fprintln(stderr, "weld: params list takes no positional args") //nolint:errcheck // final error output is best effort
		return 2
	}
	owned, ok := resolveParamsStore(nodeFlag, store, stderr)
	if !ok {
		return 2
	}
	prefixes := parseListPrefixes(*prefixesFlag)
	result := owned.List(prefixes, *depthFlag)
	return printParamsListNames(stdout, stderr, owned, result.Names, *typesFlag)
}

// printParamsEventParams prints new, changed, and deleted params for one event.
func printParamsEventParams(stdout, stderr io.Writer, e dds.ParameterEvent) int {
	for _, p := range e.NewParams {
		_, err := fmt.Fprintf(stdout, "  new %s=%s (%s)\n", p.Name, formatParamsValue(p.Value), paramsTypeName(p.Value.Type))
		if err != nil {
			_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
			return 1
		}
	}
	for _, p := range e.ChangedParams {
		_, err := fmt.Fprintf(stdout, "  changed %s=%s (%s)\n", p.Name, formatParamsValue(p.Value), paramsTypeName(p.Value.Type))
		if err != nil {
			_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
			return 1
		}
	}
	for _, p := range e.DeletedParams {
		_, err := fmt.Fprintf(stdout, "  deleted %s\n", p.Name)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
			return 1
		}
	}
	return 0
}

// printParamsEvent prints one event header plus its params.
func printParamsEvent(stdout, stderr io.Writer, e dds.ParameterEvent) int {
	_, err := fmt.Fprintf(stdout, "node=%s new=%d changed=%d deleted=%d\n", e.Node, len(e.NewParams), len(e.ChangedParams), len(e.DeletedParams))
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}
	return printParamsEventParams(stdout, stderr, e)
}

// runParamsEventsCommand prints recorded events.
func runParamsEventsCommand(args []string, stdout, stderr io.Writer, store *dds.MemoryStore) int {
	fs := flag.NewFlagSet("params events", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if isHelpRequest(args) {
		printParamsUsage(stdout)
		return 0
	}
	nodeFlag := paramsNodeFlag(fs)
	err := fs.Parse(args)
	if err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		_, _ = fmt.Fprintln(stderr, "weld: params events takes no positional args") //nolint:errcheck // final error output is best effort
		return 2
	}
	owned, ok := resolveParamsStore(nodeFlag, store, stderr)
	if !ok {
		return 2
	}
	events := owned.Events()
	for _, e := range events {
		code := printParamsEvent(stdout, stderr, e)
		if code != 0 {
			return code
		}
	}
	return 0
}

// yamlQuote quotes a string for dump output.
func yamlQuote(s string) string {
	quoted := strconv.Quote(s)
	return quoted
}

// yamlDouble renders a double with trailing .0 for YAML.
func yamlDouble(v float64) string {
	text := strconv.FormatFloat(v, 'g', -1, 64)
	if !strings.ContainsAny(text, ".eE") {
		text += ".0"
	}
	return text
}

// yamlArrayValue renders array param values for dump output.
func yamlArrayValue(value dds.ParamValue) string {
	switch value.Type {
	case dds.ParamByteArray:
		parts := make([]string, 0, len(value.ByteArray))
		for _, b := range value.ByteArray {
			parts = append(parts, strconv.Itoa(int(b)))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case dds.ParamBoolArray:
		parts := make([]string, 0, len(value.BoolArray))
		for _, b := range value.BoolArray {
			parts = append(parts, strconv.FormatBool(b))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case dds.ParamIntegerArray:
		parts := make([]string, 0, len(value.IntegerArray))
		for _, v := range value.IntegerArray {
			parts = append(parts, strconv.FormatInt(v, 10))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case dds.ParamDoubleArray:
		parts := make([]string, 0, len(value.DoubleArray))
		for _, v := range value.DoubleArray {
			parts = append(parts, yamlDouble(v))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case dds.ParamStringArray:
		parts := make([]string, 0, len(value.StringArray))
		for _, v := range value.StringArray {
			parts = append(parts, yamlQuote(v))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	default:
		return ""
	}
}

// yamlValue renders one ParamValue as YAML.
func yamlValue(value dds.ParamValue) string {
	switch value.Type {
	case dds.ParamBool:
		return strconv.FormatBool(value.Bool)
	case dds.ParamInteger:
		return strconv.FormatInt(value.Integer, 10)
	case dds.ParamDouble:
		return yamlDouble(value.Double)
	case dds.ParamString:
		return yamlQuote(value.String)
	case dds.ParamByteArray, dds.ParamBoolArray, dds.ParamIntegerArray, dds.ParamDoubleArray, dds.ParamStringArray:
		return yamlArrayValue(value)
	default:
		return "null"
	}
}

// dumpParams writes canonical YAML for the store.
func dumpParams(store *dds.MemoryStore, w io.Writer) error {
	result := store.List(nil, dds.ParamDepthRecursive)
	sort.Strings(result.Names)
	node := store.NodeName()
	_, err := fmt.Fprintf(w, "%s:\n  ros__parameters:\n", node)
	if err != nil {
		return fmt.Errorf("weld: dump header: %w", err)
	}
	for _, n := range result.Names {
		value, present := store.Get(n)
		if !present {
			continue
		}
		if value.Type == dds.ParamNotSet {
			continue
		}
		_, err = fmt.Fprintf(w, "    %s: %s\n", n, yamlValue(value))
		if err != nil {
			return fmt.Errorf("weld: dump %q: %w", n, err)
		}
	}
	return nil
}

// stripYAMLComment removes trailing comments outside quotes.
func stripYAMLComment(line string) string {
	inSingle := false
	inDouble := false
	for i := range line {
		c := line[i]
		if c == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if c == '"' && !inSingle {
			if i > 0 && line[i-1] == '\\' {
				continue
			}
			inDouble = !inDouble
			continue
		}
		if c == '#' && !inSingle && !inDouble {
			return strings.TrimRight(line[:i], " \t")
		}
	}
	return line
}

// parseFlowArray parses bracketed flow values.
func parseFlowArray(inner string) []string {
	trimmed := strings.TrimSpace(inner)
	if trimmed == "" {
		return []string{}
	}
	var parts []string
	var cur strings.Builder
	inSingle := false
	inDouble := false
	for i := range trimmed {
		c := trimmed[i]
		if c == '\'' && !inDouble {
			inSingle = !inSingle
			_ = cur.WriteByte(c) //nolint:errcheck // strings.Builder writes never fail
			continue
		}
		if c == '"' && !inSingle {
			if i > 0 && trimmed[i-1] == '\\' {
				_ = cur.WriteByte(c) //nolint:errcheck // strings.Builder writes never fail
				continue
			}
			inDouble = !inDouble
			_ = cur.WriteByte(c) //nolint:errcheck // strings.Builder writes never fail
			continue
		}
		if c == ',' && !inSingle && !inDouble {
			parts = append(parts, strings.TrimSpace(cur.String()))
			cur.Reset()
			continue
		}
		_ = cur.WriteByte(c) //nolint:errcheck // strings.Builder writes never fail
	}
	parts = append(parts, strings.TrimSpace(cur.String()))
	return parts
}

// isQuotedString reports whether s is wrapped in matching quotes.
func isQuotedString(s string) bool {
	if len(s) < 2 {
		return false
	}
	if strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"") {
		return true
	}
	return strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")
}

// unquoteYAMLString removes surrounding quotes with unquote fallback.
func unquoteYAMLString(t string) string {
	if len(t) >= 2 && ((t[0] == '"' && t[len(t)-1] == '"') || (t[0] == '\'' && t[len(t)-1] == '\'')) {
		unquoted, err := strconv.Unquote(t)
		if err != nil {
			unquoted = t[1 : len(t)-1]
		}
		return unquoted
	}
	return t
}

// inferYAMLStringArray builds a string array from flow items.
func inferYAMLStringArray(items []string) (dds.ParamValue, error) {
	out := make([]string, 0, len(items))
	for _, item := range items {
		t := strings.TrimSpace(item)
		out = append(out, unquoteYAMLString(t))
	}
	return dds.ParamValue{Type: dds.ParamStringArray, StringArray: out}, nil
}

// inferYAMLBoolArray builds a bool array from flow items.
func inferYAMLBoolArray(items []string) (dds.ParamValue, error) {
	out := make([]bool, 0, len(items))
	for _, item := range items {
		b, err := parseParamsBool(item)
		if err != nil {
			return dds.ParamValue{}, err
		}
		out = append(out, b)
	}
	return dds.ParamValue{Type: dds.ParamBoolArray, BoolArray: out}, nil
}

// inferYAMLDoubleArray builds a double array from flow items.
func inferYAMLDoubleArray(items []string) (dds.ParamValue, error) {
	out := make([]float64, 0, len(items))
	for _, item := range items {
		v, err := strconv.ParseFloat(strings.TrimSpace(item), 64)
		if err != nil {
			return dds.ParamValue{}, fmt.Errorf("weld: invalid double %q: %w", item, err)
		}
		out = append(out, v)
	}
	return dds.ParamValue{Type: dds.ParamDoubleArray, DoubleArray: out}, nil
}

// inferYAMLIntegerArray builds an integer array from flow items.
func inferYAMLIntegerArray(items []string) (dds.ParamValue, error) {
	out := make([]int64, 0, len(items))
	for _, item := range items {
		v, err := strconv.ParseInt(strings.TrimSpace(item), 10, 64)
		if err != nil {
			return dds.ParamValue{}, fmt.Errorf("weld: invalid integer %q: %w", item, err)
		}
		out = append(out, v)
	}
	return dds.ParamValue{Type: dds.ParamIntegerArray, IntegerArray: out}, nil
}

// flowItemsAreDouble reports whether any flow item looks like a double.
func flowItemsAreDouble(items []string) bool {
	for _, item := range items {
		t := strings.TrimSpace(item)
		if strings.ContainsAny(t, ".eE") {
			return true
		}
	}
	return false
}

// inferYAMLIntOrDoubleArray picks integer or double array for int-like first.
func inferYAMLIntOrDoubleArray(items []string) (dds.ParamValue, error) {
	if flowItemsAreDouble(items) {
		return inferYAMLDoubleArray(items)
	}
	return inferYAMLIntegerArray(items)
}

// inferYAMLPlainStringArray builds a trimmed string array from flow items.
func inferYAMLPlainStringArray(items []string) (dds.ParamValue, error) {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, strings.TrimSpace(item))
	}
	return dds.ParamValue{Type: dds.ParamStringArray, StringArray: out}, nil
}

// inferYAMLFlow infers a ParamValue from bracketed flow text.
func inferYAMLFlow(trimmed string) (dds.ParamValue, error) {
	inner := trimmed[1 : len(trimmed)-1]
	items := parseFlowArray(inner)
	if len(items) == 0 {
		return dds.ParamValue{Type: dds.ParamStringArray, StringArray: []string{}}, nil
	}
	first := strings.TrimSpace(items[0])
	if (strings.HasPrefix(first, "\"") && strings.HasSuffix(first, "\"")) || (strings.HasPrefix(first, "'") && strings.HasSuffix(first, "'")) {
		return inferYAMLStringArray(items)
	}
	lowered := strings.ToLower(first)
	if lowered == "true" || lowered == "false" {
		return inferYAMLBoolArray(items)
	}
	if _, err := strconv.ParseInt(first, 10, 64); err == nil {
		return inferYAMLIntOrDoubleArray(items)
	}
	if _, err := strconv.ParseFloat(first, 64); err == nil {
		return inferYAMLDoubleArray(items)
	}
	return inferYAMLPlainStringArray(items)
}

// inferYAMLScalar infers a ParamValue from non-flow text.
func inferYAMLScalar(trimmed string) (dds.ParamValue, error) {
	if isQuotedString(trimmed) {
		unquoted, err := strconv.Unquote(trimmed)
		if err != nil {
			unquoted = trimmed[1 : len(trimmed)-1]
		}
		return dds.ParamValue{Type: dds.ParamString, String: unquoted}, nil
	}
	lowered := strings.ToLower(trimmed)
	if lowered == "true" || lowered == "false" {
		b, err := parseParamsBool(trimmed)
		if err != nil {
			return dds.ParamValue{}, err
		}
		return dds.ParamValue{Type: dds.ParamBool, Bool: b}, nil
	}
	if v, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return dds.ParamValue{Type: dds.ParamInteger, Integer: v}, nil
	}
	if v, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return dds.ParamValue{Type: dds.ParamDouble, Double: v}, nil
	}
	return dds.ParamValue{Type: dds.ParamString, String: trimmed}, nil
}

// inferYAMLValue infers a ParamValue from YAML text.
func inferYAMLValue(text string) (dds.ParamValue, error) {
	trimmed := strings.TrimSpace(stripYAMLComment(text))
	if trimmed == "" || trimmed == "null" || trimmed == "~" {
		return dds.ParamValue{Type: dds.ParamNotSet}, nil
	}
	if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
		return inferYAMLFlow(trimmed)
	}
	return inferYAMLScalar(trimmed)
}

// loadSectionState updates inParams for header lines.
//
// It returns the new inParams value and whether the line is a header that
// should be skipped. Non-header lines return skip false so the caller
// continues with param parsing.
func loadSectionState(raw, trimmed string, inParams bool) (bool, bool) {
	if !strings.HasSuffix(trimmed, ":") || strings.HasPrefix(strings.TrimSpace(raw), "-") {
		return inParams, false
	}
	name := strings.TrimSpace(strings.TrimSuffix(trimmed, ":"))
	if name == "ros__parameters" {
		return true, true
	}
	return inParams, true
}

// parseLoadLine parses one indented param line into a Parameter.
func parseLoadLine(line, raw string) (dds.Parameter, error) {
	name, valueText, ok := strings.Cut(line, ":")
	if !ok {
		return dds.Parameter{}, fmt.Errorf("weld: load line %q misses colon", raw)
	}
	name = strings.TrimSpace(name)
	valueText = strings.TrimSpace(valueText)
	if name == "" {
		return dds.Parameter{}, fmt.Errorf("weld: load line %q has empty name", raw)
	}
	value, err := inferYAMLValue(valueText)
	if err != nil {
		return dds.Parameter{}, fmt.Errorf("weld: load %q: %w", name, err)
	}
	return dds.Parameter{Name: name, Value: value}, nil
}

// applyLoadedParams writes parsed params to the store.
func applyLoadedParams(store *dds.MemoryStore, params []dds.Parameter) error {
	if len(params) == 0 {
		return nil
	}
	results := store.SetMany(params)
	for i, res := range results {
		if !res.Successful {
			return fmt.Errorf("weld: load %q: %s", params[i].Name, res.Reason)
		}
	}
	return nil
}

// loadParams reads canonical YAML and applies it to the store.
func loadParams(store *dds.MemoryStore, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("weld: load read: %w", err)
	}
	lines := strings.Split(string(data), "\n")
	inParams := false
	var params []dds.Parameter
	for _, raw := range lines {
		line := stripYAMLComment(raw)
		if strings.TrimSpace(line) == "" {
			continue
		}
		trimmed := strings.TrimSpace(line)
		updated, skip := loadSectionState(raw, trimmed, inParams)
		inParams = updated
		if skip {
			continue
		}
		if !inParams {
			continue
		}
		indent := len(raw) - len(strings.TrimLeft(raw, " \t"))
		if indent < 4 {
			continue
		}
		param, err := parseLoadLine(line, raw)
		if err != nil {
			return err
		}
		params = append(params, param)
	}
	return applyLoadedParams(store, params)
}

// runParamsDumpCommand writes canonical YAML to stdout or file.
func runParamsDumpCommand(args []string, stdout, stderr io.Writer, store *dds.MemoryStore) int {
	fs := flag.NewFlagSet("params dump", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if isHelpRequest(args) {
		printParamsUsage(stdout)
		return 0
	}
	nodeFlag := paramsNodeFlag(fs)
	fileFlag := fs.String("file", "", "dump file, empty means stdout")
	err := fs.Parse(args)
	if err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		_, _ = fmt.Fprintln(stderr, "weld: params dump takes no positional args") //nolint:errcheck // final error output is best effort
		return 2
	}
	owned, ok := resolveParamsStore(nodeFlag, store, stderr)
	if !ok {
		return 2
	}
	if *fileFlag == "" {
		err = dumpParams(owned, stdout)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
			return 1
		}
		return 0
	}
	file, err := os.Create(*fileFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}
	defer func() {
		_ = file.Close() //nolint:errcheck // close error reported via sync below
	}()
	err = dumpParams(owned, file)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}
	err = file.Sync()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}
	return 0
}

// runParamsLoadCommand reads canonical YAML from file or stdin.
func runParamsLoadCommand(args []string, stdout, stderr io.Writer, store *dds.MemoryStore) int {
	fs := flag.NewFlagSet("params load", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if isHelpRequest(args) {
		printParamsUsage(stdout)
		return 0
	}
	nodeFlag := paramsNodeFlag(fs)
	fileFlag := fs.String("file", "", "load file, empty means stdin")
	err := fs.Parse(args)
	if err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		_, _ = fmt.Fprintln(stderr, "weld: params load takes no positional args") //nolint:errcheck // final error output is best effort
		return 2
	}
	owned, ok := resolveParamsStore(nodeFlag, store, stderr)
	if !ok {
		return 2
	}
	if *fileFlag == "" {
		err = loadParams(owned, os.Stdin)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
			return 1
		}
		return 0
	}
	data, err := os.ReadFile(*fileFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}
	err = loadParams(owned, strings.NewReader(string(data)))
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}
	return 0
}
