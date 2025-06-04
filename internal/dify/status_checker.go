package dify

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/step-chen/dify-atlassian-go/internal/batchpool"
)

// CheckBatchStatus checks the batch status using Dify client
// This is a method on Client since it uses client-specific operations
func (c *Client) CheckBatchStatus(ctx context.Context, key, id, title, batch, source string, op batchpool.Operation, indexingTimeout int, deleteTimeoutContent bool) (string, batchpool.Operation, error) {
	// Check for context cancellation first (from BatchPool's task timeout or global shutdown)
	select {
	case <-ctx.Done():
		return "", op, ctx.Err() // Propagate context error, return original op
	default:
		// Proceed with status check if context is not done
	}

	status, err := c.GetIndexingStatus(batch)
	if err != nil {
		return_status := ""
		if err.Error() == "unexpected status code: 404" { // TODO: make this more robust than string matching
			return_status = "completed"
		}
		return return_status, op, err
	}
	if len(status.Data) > 0 {
		if status.Data[0].IndexingStatus == "completed" {
			return "completed", op, nil
		}
		op.StartAt = status.LastStepAt()

		// Check if ProcessingStartedAt is more than the specified timeout
		if time.Since(op.StartAt) > time.Duration(indexingTimeout)*time.Minute {
			fmt.Println("timeout:", op.StartAt, time.Since(op.StartAt), time.Now())

			if deleteTimeoutContent {
				// Delete the document or update metadata if configured to do so
				// Pass the confluenceID which is a parameter of statusChecker
				err := c.DeleteDocument(status.Data[0].ID, source, id)
				if err != nil {
					// Error message updated to reflect potential metadata update failure too
					return "", op, fmt.Errorf("failed to delete/update timeout document %s for %s content %s: %w", status.Data[0].ID, key, title, err)
				}

				// Store and log the ID for retry
				if op.Action == batchpool.ActionCreate {
					op.Action = batchpool.ActionUpdate // Mark as needing retry (deletion happened, so next attempt might be update or re-create)
				}
				return "deleted", op, nil // Marked as deleted, will be retried later
			} else {
				// If not configured to delete, simply mark as completed and don't retry
				log.Printf("Indexing timed out for %s (%s), but configured not to delete. Marking as completed.", title, id)
				return "timeout", op, nil
			}
		}
		return status.Data[0].IndexingStatus, op, nil
	}

	return "", op, fmt.Errorf("no status data found")
}
