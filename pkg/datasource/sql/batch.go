/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sql

import (
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"strings"
)

var (
	errNilBatchDB      = errors.New("batch db is nil")
	errNilBatchTx      = errors.New("batch tx is nil")
	errEmptyBatchQuery = errors.New("batch query is empty")
)

// batchExecContext describes one semantic batch.
// A batch contains exactly one SQL template and an ordered set of argument groups.
// Its lifetime is limited to one batch invocation.
type batchExecContext struct {
	query     string
	batchArgs [][]any
}

func newBatchExecContext(ctx context.Context, query string, batchArgs [][]any) (*batchExecContext, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if strings.TrimSpace(query) == "" {
		return nil, errEmptyBatchQuery
	}

	return &batchExecContext{query: query, batchArgs: batchArgs}, nil
}

// ExecBatchContext executes one SQL template with multiple argument groups.
//
// The transaction is owned by this function.
// All batch items are executed sequentially in one local transaction.
// The first execution error stops the batch and causes the whole transaction to be rolled back.
//
// This is the batch counterpart of database/sql.DB.ExecContext:
// when callers need to combine the batch with other statements in the same transaction,
// they should begin a transaction explicitly and use ExecBatchInTxContext.
func ExecBatchContext(ctx context.Context, db *gosql.DB, query string, batchArgs [][]any) error {
	if db == nil {
		return errNilBatchDB
	}

	batchCtx, err := newBatchExecContext(ctx, query, batchArgs)
	if err != nil {
		return err
	}

	if len(batchCtx.batchArgs) == 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin batch transaction: %w", err)
	}

	// database/sql may already have rolled back the transaction after context cancellation.
	// Do not let ErrTxDone hide the original execution error.
	if err := executeBatch(ctx, tx, batchCtx); err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, gosql.ErrTxDone) {
			return errors.Join(err, fmt.Errorf("rollback batch transaction: %w", rollbackErr))
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch transaction: %w", err)
	}
	return nil
}

// ExecBatchInTxContext executes one SQL template with multiple argument groups
// inside a caller-owned transaction.
//
// The function never commits or rolls back tx.
// If an item fails, execution stops immediately and the error is returned to the caller,
// which remains responsible for the transaction lifecycle.
func ExecBatchInTxContext(ctx context.Context, tx *gosql.Tx, query string, batchArgs [][]any) error {
	if tx == nil {
		return errNilBatchTx
	}

	batchCtx, err := newBatchExecContext(ctx, query, batchArgs)
	if err != nil {
		return err
	}

	if len(batchCtx.batchArgs) == 0 {
		return nil
	}

	return executeBatch(ctx, tx, batchCtx)
}

// executeBatch is the semantic batch execution core.
//
// Regardless of whether the transaction was created by ExecBatchContext or
// supplied by the caller, all items are executed on the same *sql.Tx and
// therefore the same underlying database transaction.
func executeBatch(ctx context.Context, tx *gosql.Tx, batchCtx *batchExecContext) error {
	for i, arg := range batchCtx.batchArgs {
		if _, err := tx.ExecContext(ctx, batchCtx.query, arg...); err != nil {
			return fmt.Errorf("execute batch item %d: %w", i, err)
		}
	}
	return nil
}
