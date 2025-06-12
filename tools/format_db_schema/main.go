package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// go run main.go -input input.txt -output output
func main() {
	inputFilePath := flag.String("input", "", "Path to the input file containing database schema.")
	outputDirPath := flag.String("output", "", "Path to the output directory to store formatted schema files.")
	flag.Parse()

	if *inputFilePath == "" || *outputDirPath == "" {
		fmt.Println("Usage: go run main.go -input <input_file_path> -output <output_directory_path>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if err := processSchemaFile(*inputFilePath, *outputDirPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error processing schema file: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Schema formatting completed successfully.")
}

func processSchemaFile(inputPath, outputPath string) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	var schemas map[string]*Schema // Correct type for parsedSchemas

	// Call the modified ParseSchema from schema_parser.go
	parsedSchemas, err := ParseSchema(file) // Assumes ParseSchema is in the same package 'main'
	if err != nil {
		return fmt.Errorf("failed to parse schema: %w", err)
	}
	schemas = parsedSchemas // Assign to the schemas map used for output

	// Write schemas to output files
	if err := os.MkdirAll(outputPath, 0755); err != nil {
		return fmt.Errorf("failed to create output directory %s: %w", outputPath, err)
	}
	for dbName, schema := range schemas {
		outputFileName := fmt.Sprintf("%s.sql", dbName)
		outputFilePath := filepath.Join(outputPath, outputFileName)

		outputFile, err := os.Create(outputFilePath)
		if err != nil {
			return fmt.Errorf("failed to create output file %s: %w", outputFilePath, err)
		}
		// Defer close with error handling
		closeFile := func(f *os.File, path string) {
			if err := f.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "Error closing file %s: %v\n", path, err)
			}
		}
		defer closeFile(outputFile, outputFilePath)

		writer := bufio.NewWriter(outputFile)
		fmt.Fprintf(writer, "## %s.db\n", dbName) // Output format requires .db suffix

		// Reconstruct and write CREATE TABLE statements
		for _, table := range schema.Tables {
			var sb strings.Builder
			sb.WriteString(fmt.Sprintf("CREATE TABLE %s.%s (", dbName, table.Name))
			if len(table.Columns) > 0 || len(table.Constraints) > 0 {
				sb.WriteString("\n")
			}

			needsComma := false
			for _, col := range table.Columns {
				if needsComma {
					sb.WriteString(",\n")
				}
				sb.WriteString(fmt.Sprintf("  %s %s", col.Name, col.Type))
				needsComma = true
			}

			for _, constraint := range table.Constraints {
				if needsComma {
					sb.WriteString(",\n")
				}
				sb.WriteString(fmt.Sprintf("  %s", constraint))
				needsComma = true
			}
			if len(table.Columns) > 0 || len(table.Constraints) > 0 { // Add newline before closing parenthesis if there was content
				sb.WriteString("\n")
			}
			sb.WriteString(")")
			fmt.Fprintf(writer, "%s;\n\n", sb.String())
		}

		// Reconstruct and write CREATE INDEX statements
		for _, index := range schema.Indexes {
			var sb strings.Builder
			sb.WriteString("CREATE ")
			if index.IsUnique {
				sb.WriteString("UNIQUE ")
			}
			sb.WriteString(fmt.Sprintf("INDEX %s.%s ON %s.%s (", dbName, index.Name, dbName, index.Table))
			sb.WriteString(strings.Join(index.Columns, ", "))
			sb.WriteString(")")
			fmt.Fprintf(writer, "%s;\n\n", sb.String())
		}

		// Write other DDL statements (Triggers, Views)
		for _, stmt := range schema.OtherDDLs {
			fmt.Fprintf(writer, "%s\n\n", stmt.RawDDL) // Assumes RawDDL includes trailing semicolon
		}

		if err := writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush output for %s: %w", dbName, err)
		}
	}
	return nil
}
