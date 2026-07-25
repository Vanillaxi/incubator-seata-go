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
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestExecBatchContextCommitOnSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	query := "UPDATE user SET name = ? WHERE id = ?"

	mock.ExpectBegin()

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("user1", 1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("user2", 2).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("user3", 3).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	err = ExecBatchContext(ctx, db, query, [][]any{{"user1", 1}, {"user2", 2}, {"user3", 3}})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecBatchContextRollbackOnItemFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	query := "UPDATE user SET name = ? WHERE id = ?"
	execErr := errors.New("execute failed")

	mock.ExpectBegin()

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("user1", 1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("user2", 2).
		WillReturnError(execErr)

	mock.ExpectRollback()

	err = ExecBatchContext(ctx, db, query, [][]any{{"user1", 1}, {"user2", 2}, {"user3", 3}})

	require.Error(t, err)
	require.ErrorIs(t, err, execErr)
	require.Contains(t, err.Error(), "batch item 1")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecBatchInTxContextKeepsCallerTransactionOpen(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	batchQuery := "UPDATE user SET name = ? WHERE id=?"
	singleQuery := "UPDATE account SET balance = ? WHERE id = ?"

	mock.ExpectBegin()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta(batchQuery)).
		WithArgs("user1", 1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec(regexp.QuoteMeta(batchQuery)).
		WithArgs("user2", 2).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = ExecBatchInTxContext(ctx, tx, batchQuery, [][]any{{"user1", 1}, {"user2", 2}})
	require.NoError(t, err)

	// If the batch API committed the transaction internally,this statement would fail with sql.ErrTxDone
	mock.ExpectExec(regexp.QuoteMeta(singleQuery)).
		WithArgs(100, 10).
		WillReturnResult(sqlmock.NewResult(0, 1))

	_, err = tx.ExecContext(ctx, singleQuery, 100, 10)
	require.NoError(t, err)

	mock.ExpectCommit()
	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecBatchInTxContextDoesNotRollbackCallerTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	query := "UPDATE user SET name = ? WHERE id = ?"
	execErr := errors.New("execute failed")

	mock.ExpectBegin()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("user1", 1).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectExec(regexp.QuoteMeta(query)).
		WithArgs("user2", 2).
		WillReturnError(execErr)

	err = ExecBatchInTxContext(ctx, tx, query, [][]any{{"user1", 1}, {"user2", 2}})
	require.ErrorIs(t, err, execErr)

	// The caller still owns the transaction
	mock.ExpectRollback()

	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecBatchContextEmptyBatchIsNoop(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	err = ExecBatchContext(context.Background(), db, "UPDATE user SET name = ? WHERE id = ?", nil)
	require.NoError(t, err)
	// No Begin/Exec/Commit should happen.
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExecBatchContextDoesNotLeakStateAfterFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	query := "UPDATE user SET name = ? WHERE id = ?"

	firstBatchErr := errors.New("first batch failed")

	// Batch A.
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs("usera1", 1).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs("usera2", 2).WillReturnError(firstBatchErr)
	mock.ExpectRollback()

	err = ExecBatchContext(ctx, db, query, [][]any{{"usera1", 1}, {"usera2", 2}})
	require.ErrorIs(t, err, firstBatchErr)

	// Batch B.
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs("userb1", 3).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(query)).WithArgs("userb2", 4).WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	err = ExecBatchContext(ctx, db, query, [][]any{{"userb1", 3}, {"userb2", 4}})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
