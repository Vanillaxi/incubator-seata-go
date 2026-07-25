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
	"database/sql/driver"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"seata.apache.org/seata-go/v2/pkg/datasource/sql/mock"
	"seata.apache.org/seata-go/v2/pkg/protocol/branch"
	"seata.apache.org/seata-go/v2/pkg/tm"
)

func newBatchATTestDB(t *testing.T,
	ctrl *gomock.Controller,
) (*gosql.DB, *mock.MockTestDriverConn, *mock.MockTestDriverTx) {
	t.Helper()

	_ = initMockResourceManager(branch.BranchTypeAT, ctrl)

	db, err := gosql.Open(
		SeataATMySQLDriver,
		"root:12345678@tcp(127.0.0.1:3306)/seata_client?multiStatements=true",
	)
	require.NoError(t, err)

	mockTx := mock.NewMockTestDriverTx(ctrl)
	mockConn := mock.NewMockTestDriverConn(ctrl)

	mockConn.EXPECT().
		QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			rows := &mysqlMockRows{}
			rows.data = [][]interface{}{
				{"8.0.29"},
			}
			return rows, nil
		})

	mockConn.EXPECT().ResetSession(gomock.Any()).AnyTimes().Return(nil)
	mockConn.EXPECT().Close().AnyTimes().Return(nil)

	connector := mock.NewMockTestDriverConnector(ctrl)
	connector.EXPECT().Connect(gomock.Any()).AnyTimes().Return(mockConn, nil)

	_ = initMockAtConnector(t, ctrl, db, func(t *testing.T, ctrl *gomock.Controller) driver.Connector {
		return connector
	})

	return db, mockConn, mockTx
}

func TestExecBatchContextGlobalATUsesSingleLocalTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	db, mockConn, mockTx := newBatchATTestDB(t, ctrl)
	defer db.Close()

	ctx := tm.InitSeataContext(context.Background())
	tm.SetXID(ctx, uuid.NewString())

	query := "SELECT ?"
	var executedArgs []any

	// The whole batch must create exactly one local transaction
	mockConn.EXPECT().BeginTx(gomock.Any(), gomock.Any()).Times(1).Return(mockTx, nil)
	mockConn.EXPECT().ExecContext(gomock.Any(), query, gomock.Any()).Times(3).DoAndReturn(func(
		ctx context.Context, query string, args []driver.NamedValue,
	) (driver.Result, error) {
		require.Len(t, args, 1)
		executedArgs = append(executedArgs, args[0].Value)
		return driver.ResultNoRows, nil
	})

	// All items belong to the same transaction,so commit only once
	mockTx.EXPECT().Commit().Times(1).Return(nil)
	err := ExecBatchContext(ctx, db, query, [][]any{{"item0"}, {"item1"}, {"item2"}})

	require.NoError(t, err)
	require.Equal(t, []any{"item0", "item1", "item2"}, executedArgs)
}

func TestExecBatchContextGlobalATRollbackOwnedTransactionOnFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	db, mockConn, mockTx := newBatchATTestDB(t, ctrl)
	defer db.Close()

	ctx := tm.InitSeataContext(context.Background())
	tm.SetXID(ctx, uuid.NewString())

	query := "SELECT ?"
	execErr := errors.New("execute failed")

	var execCount int32
	mockConn.EXPECT().BeginTx(gomock.Any(), gomock.Any()).Times(1).Return(mockTx, nil)
	mockConn.EXPECT().ExecContext(gomock.Any(), query, gomock.Any()).
		Times(2).DoAndReturn(func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
		count := atomic.AddInt32(&execCount, 1)
		if count == 2 {
			return nil, execErr
		}
		return driver.ResultNoRows, nil
	})

	mockTx.EXPECT().Rollback().Times(1).Return(nil)
	err := ExecBatchContext(ctx, db, query, [][]any{{"item0"}, {"item1"}, {"item2"}})

	require.Error(t, err)
	require.ErrorIs(t, err, execErr)
	require.Contains(t, err.Error(), "batch item 1")

	// item2 must never execute
	require.Equal(t, int32(2), atomic.LoadInt32(&execCount))
}

func TestExecBatchInTxContextAllowsFollowingExecInSameTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	db, mockConn, mockTx := newBatchATTestDB(t, ctrl)
	defer db.Close()

	ctx := tm.InitSeataContext(context.Background())
	tm.SetXID(ctx, uuid.NewString())

	batchQuery := "SELECT ?"
	normalQuery := "SELECT ?"

	var executedArgs []any

	// Caller creates one transaction.
	mockConn.EXPECT().BeginTx(gomock.Any(), gomock.Any()).Times(1).Return(mockTx, nil)

	// 2 batch items + 1 ordinary SQL .
	mockConn.EXPECT().ExecContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(3).
		DoAndReturn(func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
			executedArgs = append(executedArgs, args[0].Value)
			return driver.ResultNoRows, nil
		})

	mockTx.EXPECT().Commit().Times(1).Return(nil)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	err = ExecBatchInTxContext(ctx, tx, batchQuery, [][]any{{"item0"}, {"item1"}})
	require.NoError(t, err)

	// Batch execution must not close caller-owned transaction
	_, err = tx.ExecContext(ctx, normalQuery, "normal")
	require.NoError(t, err)

	require.NoError(t, tx.Commit())
	require.Equal(t, []any{"item0", "item1", "normal"}, executedArgs)
}
