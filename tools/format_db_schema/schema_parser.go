package main

import (
	"bufio"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
)

// Column represents a single column in a database table.
// It holds the name and data type of the column.
type Column struct {
	Name string
	Type string
}

// Table represents a database table, including its name and a slice of its columns.
type Table struct {
	Name        string
	Columns     []Column
	Constraints []string // For table-level constraints like PRIMARY KEY, FOREIGN KEY, CHECK, etc.
}

// Index represents a database index, including its name, the table it belongs to,
// and the columns it covers.
type Index struct {
	Name     string
	Table    string
	Columns  []string
	IsUnique bool
}

// OtherStatement holds raw DDL for statements like CREATE TRIGGER, CREATE VIEW.
type OtherStatement struct {
	Type   string // e.g., "TRIGGER", "VIEW"
	RawDDL string
}

// Schema is a top-level container for all tables and indexes extracted from the DDL script.
type Schema struct {
	Tables    map[string]Table // Key: Table Name
	Indexes   map[string]Index // Key: Index Name
	OtherDDLs []OtherStatement // For Triggers, Views, etc.
}

var (
	// Used to detect start of new statements for splitting logic within ParseSchema
	parserUtilCreateStatementRegex = regexp.MustCompile(`(?i)^CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|UNIQUE\s+INDEX|INDEX|TRIGGER|VIEW)`)
	isNumericOnlyRegex             = regexp.MustCompile(`^[0-9]+$`)

	// Regexes for parsing specific DDL parts within a complete statement
	schemaTableRegex = regexp.MustCompile(`(?is)CREATE TABLE\s+([^\s\(]+)\s*\((.*?)\);`)
	schemaIndexRegex = regexp.MustCompile(`(?is)CREATE(?:\s+(UNIQUE))?\s+INDEX\s+([^\s]+)\s+ON\s+([^\s\(]+)\s*\((.*?)\);`)
)

// extractDatabaseName (moved from main.go)
func extractDatabaseName(fullPath string) string {
	base := filepath.Base(strings.Trim(fullPath, "#"))
	stem := strings.TrimSuffix(base, filepath.Ext(base))

	if !strings.Contains(stem, "_") {
		return stem
	}

	lastUnderscoreIndex := strings.LastIndex(stem, "_")
	potentialSuffix := stem[lastUnderscoreIndex+1:]

	if potentialSuffix != "" && !isNumericOnlyRegex.MatchString(potentialSuffix) {
		return potentialSuffix
	}

	firstUnderscoreIndex := strings.Index(stem, "_")
	// If stem contains '_', firstUnderscoreIndex will be >= 0.
	// If no underscore was found (e.g. after failing suffix logic on a stem like "name123"),
	// this would return the part before the first underscore if one exists, or the whole stem if not.
	prefix := stem[:firstUnderscoreIndex]
	return prefix
}

// splitCreateTableBody attempts to split the content of CREATE TABLE (...)
// by commas, respecting parentheses. This is a simplified approach and
// does not handle commas within quoted strings.
func splitCreateTableBody(body string) []string {
	var parts []string
	balance := 0
	currentPart := strings.Builder{}
	for _, r := range body {
		if r == '(' {
			balance++
		} else if r == ')' {
			balance--
		}

		if r == ',' && balance == 0 {
			parts = append(parts, strings.TrimSpace(currentPart.String()))
			currentPart.Reset()
		} else {
			currentPart.WriteRune(r)
		}
	}
	parts = append(parts, strings.TrimSpace(currentPart.String())) // Add the last part
	return parts
}

// _parseTableFromMatches constructs a Table struct from regex submatches.
// matches should be the result of schemaTableRegex.FindStringSubmatch().
// It returns the parsed Table and a boolean indicating if the table is valid (true)
// or should be skipped (false), e.g., if it contains a 'wrongTable' column.
func _parseTableFromMatches(matches []string) (Table, bool) {
	rawTableName := strings.TrimSpace(matches[1])
	// Handle qualified table names like "schema.table"
	if parts := strings.Split(rawTableName, "."); len(parts) > 1 {
		rawTableName = parts[len(parts)-1]
	}
	columnDefs := strings.TrimSpace(matches[2])

	table := Table{Name: rawTableName, Columns: []Column{}, Constraints: []string{}}
	definitions := splitCreateTableBody(columnDefs)

	for _, defStr := range definitions {
		trimmedDef := strings.TrimSpace(defStr)
		if trimmedDef == "" {
			continue
		}
		upperTrimmedDef := strings.ToUpper(trimmedDef)

		// Check for table-level constraints
		if strings.HasPrefix(upperTrimmedDef, "CONSTRAINT") ||
			strings.HasPrefix(upperTrimmedDef, "PRIMARY KEY") ||
			strings.HasPrefix(upperTrimmedDef, "FOREIGN KEY") ||
			(strings.HasPrefix(upperTrimmedDef, "UNIQUE") && strings.Contains(upperTrimmedDef, "(")) || // Heuristic for table-level UNIQUE constraint
			strings.HasPrefix(upperTrimmedDef, "CHECK") {
			table.Constraints = append(table.Constraints, trimmedDef)
		} else {
			// Assume it's a column definition
			firstSpaceIndex := strings.Index(trimmedDef, " ")
			if firstSpaceIndex == -1 {
				// Malformed or unhandled definition (e.g., column name without type)
				continue
			}

			colName := strings.Trim(strings.TrimSpace(trimmedDef[:firstSpaceIndex]), `"`+"`")
			colType := strings.TrimSpace(trimmedDef[firstSpaceIndex+1:])

			if colName == "wrongTable" { // Specific skip condition
				return table, false // Indicate table should be skipped
			}
			table.Columns = append(table.Columns, Column{Name: colName, Type: colType})
		}
	}

	return table, true // Table is valid
}

// _parseIndexFromMatches constructs an Index struct from regex submatches.
// matches should be the result of schemaIndexRegex.FindStringSubmatch().
func _parseIndexFromMatches(matches []string) Index {
	isUnique := strings.TrimSpace(strings.ToUpper(matches[1])) == "UNIQUE"
	indexName := strings.TrimSpace(matches[2])
	// Handle qualified index names like "schema.index"
	if parts := strings.Split(indexName, "."); len(parts) > 1 {
		indexName = parts[len(parts)-1]
	}
	rawTableName := strings.TrimSpace(matches[3])
	// Handle qualified table names like "schema.table"
	if parts := strings.Split(rawTableName, "."); len(parts) > 1 {
		rawTableName = parts[len(parts)-1]
	}
	columnListStr := strings.TrimSpace(matches[4])

	index := Index{Name: indexName, Table: rawTableName, Columns: []string{}, IsUnique: isUnique}
	columnListStr = strings.ReplaceAll(columnListStr, "\n", "") // Remove newlines from column list
	cols := strings.Split(columnListStr, ",")
	for _, col := range cols {
		col = strings.TrimSpace(strings.Trim(col, `"`+"`")) // Remove quotes from column names
		if col != "" {
			index.Columns = append(index.Columns, col)
		}
	}
	return index
}

// _processParsedStatement attempts to parse a complete SQL statement string
// and updates the given schema if a table or index is successfully parsed.
func _processParsedStatement(statementStr string, schema *Schema) {
	if schema == nil {
		return // Should not happen if called correctly
	}
	trimmedStmt := strings.TrimSpace(statementStr)
	if !strings.HasSuffix(trimmedStmt, ";") {
		trimmedStmt += ";"
	}

	tableMatches := schemaTableRegex.FindStringSubmatch(trimmedStmt)
	if len(tableMatches) == 3 { // 0: full match, 1: table name, 2: column defs
		table, isValidTable := _parseTableFromMatches(tableMatches)
		if isValidTable { // Only store if the table is valid
			schema.Tables[table.Name] = table
		}
		return
	}

	indexMatches := schemaIndexRegex.FindStringSubmatch(trimmedStmt)
	if len(indexMatches) == 5 { // 0: full, 1: UNIQUE, 2: index name, 3: table name, 4: columns
		index := _parseIndexFromMatches(indexMatches)
		// Only store the index if its corresponding table exists (i.e., was not skipped)
		if _, tableExists := schema.Tables[index.Table]; tableExists {
			schema.Indexes[index.Name] = index
		}
		return
	}

	// Handle CREATE TRIGGER and CREATE VIEW statements
	// These are stored as raw DDL.
	upperStmt := strings.ToUpper(trimmedStmt)
	if strings.HasPrefix(upperStmt, "CREATE TRIGGER") || strings.HasPrefix(upperStmt, "CREATE OR REPLACE TRIGGER") {
		if schema.OtherDDLs == nil { // Should be initialized, but good practice
			schema.OtherDDLs = make([]OtherStatement, 0)
		}
		schema.OtherDDLs = append(schema.OtherDDLs, OtherStatement{Type: "TRIGGER", RawDDL: trimmedStmt})
		return
	}
	if strings.HasPrefix(upperStmt, "CREATE VIEW") || strings.HasPrefix(upperStmt, "CREATE OR REPLACE VIEW") {
		if schema.OtherDDLs == nil { // Should be initialized, but good practice
			schema.OtherDDLs = make([]OtherStatement, 0)
		}
		schema.OtherDDLs = append(schema.OtherDDLs, OtherStatement{Type: "VIEW", RawDDL: trimmedStmt})
		return
	}
}

// ParseSchema scans an io.Reader containing SQL DDL statements (potentially for multiple databases)
// and extracts the schema structure for each database.
// It returns a map where keys are database names and values are pointers to Schema structs.
func ParseSchema(r io.Reader) (map[string]*Schema, error) {
	allSchemas := make(map[string]*Schema)
	var currentDBName string
	var currentStatement strings.Builder
	var currentSchemaForDB *Schema

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		trimmedLine := strings.TrimSpace(line)

		if strings.HasPrefix(trimmedLine, "#") && strings.HasSuffix(trimmedLine, "#") {
			if currentStatement.Len() > 0 {
				if currentSchemaForDB != nil {
					_processParsedStatement(currentStatement.String(), currentSchemaForDB)
				}
				currentStatement.Reset() // Reset after processing
			}
			currentDBName = extractDatabaseName(trimmedLine)
			if _, ok := allSchemas[currentDBName]; !ok {
				allSchemas[currentDBName] = &Schema{
					Tables:    make(map[string]Table),
					Indexes:   make(map[string]Index),
					OtherDDLs: make([]OtherStatement, 0),
				}
			}
			currentSchemaForDB = allSchemas[currentDBName]
			continue
		}

		if trimmedLine == "" {
			if currentStatement.Len() > 0 {
				if currentSchemaForDB != nil {
					_processParsedStatement(currentStatement.String(), currentSchemaForDB)
				}
				currentStatement.Reset()
			}
			continue
		}

		if parserUtilCreateStatementRegex.MatchString(trimmedLine) && currentStatement.Len() > 0 {
			if currentSchemaForDB != nil {
				_processParsedStatement(currentStatement.String(), currentSchemaForDB)
			}
			currentStatement.Reset()
		}

		if currentStatement.Len() > 0 {
			currentStatement.WriteString("\n")
		}
		currentStatement.WriteString(line)

		if strings.HasSuffix(trimmedLine, ";") {
			if currentSchemaForDB != nil {
				_processParsedStatement(currentStatement.String(), currentSchemaForDB)
			}
			currentStatement.Reset()
		}
	}
	if currentStatement.Len() > 0 {
		if currentSchemaForDB != nil {
			_processParsedStatement(currentStatement.String(), currentSchemaForDB)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading input: %w", err)
	}
	return allSchemas, nil
}
