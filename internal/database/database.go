package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/step-chen/dify-atlassian-go/internal/confluence"
	"github.com/step-chen/dify-atlassian-go/internal/utils"
)

const (
	dbName = "mapping.db"
)

// Database represents the database connection and operations
type Database struct {
	DB *sql.DB // SQL database connection
}

// MappingStatus represents the status of a document mapping
type MappingStatus int

// Mapping status constants
const (
	MappingStatusNew         MappingStatus = iota // New document that needs to be created
	MappingStatusUpToDate                         // Document is up to date and doesn't need changes
	MappingStatusNeedsUpdate                      // Document exists but needs to be updated
	MappingStatusNeedsDelete                      // Document needs to be deleted
)

// CheckMappingStatus checks the mapping status
func (d *Database) CheckMappingStatus(confluenceID, publishDate string) (MappingStatus, string, error) {
	// Check if mapping exists and get last update time and dify_id
	var currentDate, difyID sql.NullString
	query := `SELECT last_update_date, dify_id FROM confluence_mapping WHERE confluence_id = ?`
	err := d.DB.QueryRow(query, confluenceID).Scan(&currentDate, &difyID)
	if err != nil {
		if err == sql.ErrNoRows {
			return MappingStatusNew, "", nil
		}
		return MappingStatusNew, "", fmt.Errorf("failed to get mapping details: %w", err)
	}

	if !currentDate.Valid {
		return MappingStatusNew, "", nil
	}

	// Compare times using utility function
	equal, err := utils.CompareRFC3339Times(currentDate.String, publishDate)
	if err != nil {
		return MappingStatusNeedsUpdate, difyID.String, fmt.Errorf("failed to compare times: %w", err)
	}

	// If times are equal, no update is needed
	if equal {
		return MappingStatusUpToDate, "", nil
	}

	return MappingStatusNeedsUpdate, difyID.String, nil
}

// InitDB initializes the database and creates tables
// Returns:
//   - *Database: Initialized database connection
//   - error: Any error that occurred during initialization
//
// The function:
// 1. Creates the database file if it doesn't exist
// 2. Establishes a connection to the SQLite database
// 3. Creates the confluence_mapping table if it doesn't exist
func InitDB() (*Database, error) {
	// Get current working directory
	wd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("failed to get working directory: %w", err)
	}

	// Create database file path
	dbPath := filepath.Join(wd, dbName)

	// Open database connection
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Create table
	query := `CREATE TABLE IF NOT EXISTS confluence_mapping (
		confluence_id TEXT PRIMARY KEY,
		space_key TEXT,
		dify_id TEXT,
		url TEXT,
		title TEXT,
		dataset_id TEXT,
		last_update_date DATETIME
	);`

	_, err = db.Exec(query)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return &Database{DB: db}, nil
}

// SaveMapping saves the mapping between Confluence and Dify
func (d *Database) SaveMapping(confluenceID, spaceKey, difyID, url, title, datasetID, publishDate string) error {
	// Validate time format
	_, err := time.Parse(time.RFC3339, publishDate)
	if err != nil {
		return fmt.Errorf("invalid date format, expected RFC3339 (2006-01-02T15:04:05Z07:00): %w", err)
	}

	query := `INSERT OR REPLACE INTO confluence_mapping 
		(confluence_id, space_key, dify_id, url, title, dataset_id, last_update_date) 
		VALUES (?, ?, ?, ?, ?, ?, ?)`

	_, err = d.DB.Exec(query, confluenceID, spaceKey, difyID, url, title, datasetID, publishDate)
	if err != nil {
		return fmt.Errorf("failed to save mapping: %w", err)
	}

	return nil
}

type Mapping struct {
	DifyID         string
	DatasetID      string
	LastUpdateDate string
}

// InitOperationByMapping initializes content operations based on existing mappings
// Parameters:
//   - spaceKey: The Confluence space key to query mappings for
//   - contents: Map of content operations keyed by Confluence ID (will be modified directly)
//
// Returns:
//   - error: Any error that occurred during the operation
//
// The function:
// 1. Queries the database for mappings matching the given spaceKey
// 2. For each mapping:
//   - If the Confluence ID exists in contents, updates the operation with mapping data
//   - If the Confluence ID doesn't exist, adds a new operation with delete action
//
// 3. Returns any error that occurred during the process
func (d *Database) InitOperationByMapping(spaceKey string, datasetID string, contents map[string]confluence.ContentOperation) error {
	// Query database for mappings
	query := `SELECT confluence_id, dify_id, last_update_date 
		FROM confluence_mapping WHERE space_key = ? AND dataset_id = ?`

	rows, err := d.DB.Query(query, spaceKey, datasetID)
	if err != nil {
		return fmt.Errorf("failed to query mappings: %w", err)
	}
	defer rows.Close()

	// Process each mapping row
	for rows.Next() {
		var confluenceID, difyID, lastUpdateDate string
		if err := rows.Scan(&confluenceID, &difyID, &lastUpdateDate); err != nil {
			return fmt.Errorf("failed to scan mapping row: %w", err)
		}

		// Initialize operation based on mapping data
		if op, exists := contents[confluenceID]; exists {
			// Update existing operation
			op.DifyID = difyID
			op.DatasetID = datasetID

			// Compare times using utility function
			equal, err := utils.CompareRFC3339Times(op.LastModifiedDate, lastUpdateDate)
			if err != nil {
				return fmt.Errorf("failed to compare times: %w", err)
			}

			// Determine action based on time comparison
			if !equal {
				op.Action = 2 // Update if times differ
			} else {
				op.Action = 3 // No action needed if times match
			}

			contents[confluenceID] = op
		} else {
			// Add new operation for unmapped content
			contents[confluenceID] = confluence.ContentOperation{
				Action:    2, // Delete
				DifyID:    difyID,
				DatasetID: datasetID,
			}
		}
	}

	// Check for any errors during iteration
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating over mapping rows: %w", err)
	}

	return nil
}

// DeleteMapping deletes a mapping
func (d *Database) DeleteMapping(difyID string) error {
	query := `DELETE FROM confluence_mapping WHERE dify_id = ?`

	_, err := d.DB.Exec(query, difyID)
	if err != nil {
		return fmt.Errorf("failed to delete mapping: %w", err)
	}

	return nil
}

// CleanupMappings cleans up invalid mapping records
// Parameters:
//   - datasetID: The dataset ID to clean up mappings for
//   - docIDs: Map of valid document IDs
//
// Returns:
//   - error: Any error that occurred during the operation
//
// The function:
// 1. Starts a transaction
// 2. Identifies mappings that reference non-existent documents
// 3. Deletes invalid mappings in batches
// 4. Commits the transaction
func (d *Database) CleanupMappings(datasetID string, docIDs map[string]bool) error {
	// Start transaction to ensure atomic cleanup
	tx, err := d.DB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Get dify_ids that need to be deleted by comparing with valid docIDs
	var idsToDelete []string
	rows, err := tx.Query("SELECT dify_id FROM confluence_mapping WHERE dataset_id = ?", datasetID)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to query mappings: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var difyID string
		if err := rows.Scan(&difyID); err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to scan dify_id: %w", err)
		}

		if !docIDs[difyID] {
			idsToDelete = append(idsToDelete, difyID)
		}
	}

	// If there are records to delete, process them in batches
	// to avoid hitting SQLite parameter limits
	if len(idsToDelete) > 0 {
		// Delete in batches, max 500 per batch
		batchSize := 500
		for i := 0; i < len(idsToDelete); i += batchSize {
			end := i + batchSize
			if end > len(idsToDelete) {
				end = len(idsToDelete)
			}
			batch := idsToDelete[i:end]

			// Build batch delete query
			query := "DELETE FROM confluence_mapping WHERE dify_id IN (?" + strings.Repeat(",?", len(batch)-1) + ")"
			args := make([]interface{}, len(batch))
			for j, id := range batch {
				args[j] = id
			}

			// Execute batch delete
			_, err = tx.Exec(query, args...)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to delete mappings: %w", err)
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
