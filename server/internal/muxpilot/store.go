// Package muxpilot provides durable project coordination primitives. Every
// control mutation must lock and validate its fence in the same transaction.
package muxpilot

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrFence             = errors.New("muxpilot coordinator fence is stale or expired")
	ErrOperationConflict = errors.New("muxpilot operation id was reused with a different request")
)

func TokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func NewToken() (string, error) {
	var token [32]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(token[:]), nil
}

// LockFence uses database time after acquiring the lock, so waiting behind a
// takeover cannot resurrect an expired generation. Tokens never enter the feed.
func LockFence(ctx context.Context, tx pgx.Tx, workspaceID, projectID, userID string, generation int64, token string) error {
	var actualGeneration int64
	var hash, owner string
	var expires time.Time
	err := tx.QueryRow(ctx, `SELECT generation,token_hash,user_id::text,expires_at FROM muxpilot_coordinator
  WHERE workspace_id=$1 AND project_id=$2 FOR UPDATE`, workspaceID, projectID).Scan(&actualGeneration, &hash, &owner, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrFence
	}
	if err != nil {
		return err
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return err
	}
	if owner != userID || actualGeneration != generation || !expires.After(now) ||
		subtle.ConstantTimeCompare([]byte(hash), []byte(TokenHash(token))) != 1 {
		return ErrFence
	}
	return nil
}

type Operation struct {
	Status   int             `json:"status"`
	Response json.RawMessage `json:"response"`
}

// ReplayOperation is called after LockFence. Operations are globally unique;
// a retry must identify the same project, generation and normalized request.
func ReplayOperation(ctx context.Context, tx pgx.Tx, projectID, operationID string, generation int64, requestHash string) (*Operation, error) {
	var project, hash string
	var storedGeneration int64
	var op Operation
	err := tx.QueryRow(ctx, `SELECT project_id::text,generation,request_hash,status,response FROM muxpilot_operation WHERE operation_id=$1`, operationID).Scan(&project, &storedGeneration, &hash, &op.Status, &op.Response)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if project != projectID || storedGeneration != generation || hash != requestHash {
		return nil, ErrOperationConflict
	}
	return &op, nil
}

func SaveOperation(ctx context.Context, tx pgx.Tx, projectID, operationID string, generation int64, requestHash string, status int, response json.RawMessage) error {
	_, err := tx.Exec(ctx, `INSERT INTO muxpilot_operation(operation_id,project_id,generation,request_hash,status,response) VALUES($1,$2,$3,$4,$5,$6)`, operationID, projectID, generation, requestHash, status, response)
	return err
}

type Event struct {
	Sequence    int64           `json:"sequence"`
	ID          string          `json:"id"`
	ProjectID   string          `json:"project_id"`
	WorkspaceID string          `json:"workspace_id"`
	ActorType   string          `json:"actor_type"`
	ActorID     string          `json:"actor_id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	OccurredAt  time.Time       `json:"occurred_at"`
}

// FeedEvents replays a durable project cursor and always returns a JSON array.
// The caller must authorize workspace membership before invoking this helper.
func FeedEvents(ctx context.Context, pool *pgxpool.Pool, workspaceID, projectID string, after int64, limit int) ([]Event, error) {
	if limit < 1 || limit > 1000 {
		limit = 200
	}
	rows, err := pool.Query(ctx, `SELECT sequence,id::text,project_id::text,workspace_id::text,actor_type,actor_id,type,payload,occurred_at
  FROM muxpilot_event WHERE workspace_id=$1 AND project_id=$2 AND sequence>$3 ORDER BY sequence LIMIT $4`, workspaceID, projectID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]Event, 0)
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.Sequence, &e.ID, &e.ProjectID, &e.WorkspaceID, &e.ActorType, &e.ActorID, &e.Type, &e.Payload, &e.OccurredAt); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
