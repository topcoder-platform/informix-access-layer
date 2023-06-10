package com.topcoder.dal;

import com.topcoder.dal.errors.NotImplementedException;
import com.topcoder.dal.rdb.*;
import com.topcoder.dal.util.IdGenerator;
import com.topcoder.dal.util.QueryHelper;
import com.topcoder.dal.util.StreamJdbcTemplate;
import com.topcoder.dal.util.ParameterizedExpression;

import io.grpc.stub.StreamObserver;
import jdk.jshell.spi.ExecutionControl;
import net.devh.boot.grpc.server.service.GrpcService;

import static org.jooq.impl.DSL.field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Accessor for rational database like Informix.
 */
@GrpcService
public class DBAccessor extends QueryServiceGrpc.QueryServiceImplBase {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final StreamJdbcTemplate jdbcTemplate;

    private final DataSourceTransactionManager transactionManager;

    private final QueryHelper queryHelper;

    private final IdGenerator idGenerator;

    public DBAccessor(StreamJdbcTemplate jdbcTemplate, QueryHelper queryHelper, IdGenerator idGenerator) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionManager = new DataSourceTransactionManager(
                Objects.requireNonNull(jdbcTemplate.getDataSource()));
        this.transactionManager.setNestedTransactionAllowed(false);
        this.queryHelper = queryHelper;
        this.idGenerator = idGenerator;
    }

    /**
     * Execute query operation.
     *
     * @param query            The query clause, in format "from ... where ..."
     * @param params           The parameters to bind to query, may be null
     * @param returningColumns The columns to be returned
     * @return query result
     * @throws DataAccessException
     */
    public List<String[]> executeQuery(String query, String[] params, String[] returningColumns)
            throws DataAccessException {
        String sql = "select " + String.join(",", returningColumns) + " " + query;

        return jdbcTemplate.query(sql, (rs, _rowNum) -> {
            String[] rowResult = new String[returningColumns.length];

            for (int idx = 0; idx < returningColumns.length; idx++) {
                Object value = rs.getObject(idx + 1);
                rowResult[idx] = value == null ? null : value.toString();
            }
            return rowResult;
        }, (Object[]) params);
    }

    /**
     * Execute update operation.
     *
     * @param query  The query clause
     * @param params The parameters to bind to query, may be null
     * @return the number of rows affected
     * @throws DataAccessException
     */
    public int executeUpdate(String query, String[] params) throws DataAccessException {
        return jdbcTemplate.update(query, (Object[]) params);
    }

    /**
     * Execute batch update operations in transaction.
     *
     * @param query The query clauses
     * @return the number of rows affected
     * @throws DataAccessException
     */
    @Transactional
    public int[] executeBatchUpdate(String[] query, String[][] params) throws DataAccessException {
        int size = query.length;
        if (size != params.length) {
            throw new IllegalArgumentException("Query array and params array must have same length");
        }
        int[] result = new int[size];
        for (int i = 0; i < size; i++) {
            result[i] = jdbcTemplate.update(query[i], (Object[]) params[i]);
        }
        return result;
    }

    private Row rawQueryMapper(ResultSet rs, int rowNum) throws SQLException {
        Row.Builder rowBuilder = Row.newBuilder();
        Value.Builder valueBuilder = Value.newBuilder();
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
            String columnName = rs.getMetaData().getColumnName(i + 1);
            switch (rs.getMetaData().getColumnType(i + 1)) {
                case java.sql.Types.DECIMAL ->
                    valueBuilder.setStringValue(Objects.requireNonNullElse(rs.getBigDecimal(i + 1), "").toString());
                case java.sql.Types.INTEGER -> valueBuilder.setIntValue(rs.getInt(i + 1));
                case java.sql.Types.BIGINT -> valueBuilder.setLongValue(rs.getLong(i + 1));
                case java.sql.Types.FLOAT -> valueBuilder.setFloatValue(rs.getFloat(i + 1));
                case java.sql.Types.DOUBLE -> valueBuilder.setDoubleValue(rs.getDouble(i + 1));
                case java.sql.Types.VARCHAR ->
                    valueBuilder.setStringValue(Objects.requireNonNullElse(rs.getString(i + 1), ""));
                case java.sql.Types.BOOLEAN -> valueBuilder.setBooleanValue(rs.getBoolean(i + 1));
                case java.sql.Types.DATE, java.sql.Types.TIMESTAMP -> valueBuilder
                        .setDateValue(Objects.requireNonNullElse(rs.getTimestamp(i + 1), "").toString());
                default -> throw new IllegalArgumentException(
                        "Unsupported column type: " + rs.getMetaData().getColumnType(i + 1));
            }
            rowBuilder.putValues(columnName, valueBuilder.build());
        }
        return rowBuilder.build();
    }

    private Row selectQueryMapper(ResultSet rs, int rowNum, int numColumns, ColumnType[] columnTypeMap,
            List<Column> columnList)
            throws SQLException {
        Row.Builder rowBuilder = Row.newBuilder();
        Value.Builder valueBuilder = Value.newBuilder();

        for (int i = 0; i < numColumns; i++) {
            switch (columnTypeMap[i]) {
                case COLUMN_TYPE_INT -> valueBuilder.setIntValue(rs.getInt(i + 1));
                case COLUMN_TYPE_LONG -> valueBuilder.setLongValue(rs.getLong(i + 1));
                case COLUMN_TYPE_FLOAT -> valueBuilder.setFloatValue(rs.getFloat(i + 1));
                case COLUMN_TYPE_DOUBLE -> valueBuilder.setDoubleValue(rs.getDouble(i + 1));
                case COLUMN_TYPE_STRING ->
                    valueBuilder.setStringValue(Objects.requireNonNullElse(rs.getString(i + 1), ""));
                case COLUMN_TYPE_BOOLEAN -> valueBuilder.setBooleanValue(rs.getBoolean(i + 1));
                case COLUMN_TYPE_DATE, COLUMN_TYPE_DATETIME -> valueBuilder
                        .setDateValue(Objects.requireNonNullElse(rs.getTimestamp(i + 1), "").toString());
                default ->
                    throw new IllegalArgumentException(
                            "Unsupported column type: " + i + ": " + columnTypeMap[i]);
            }

            rowBuilder.putValues(columnList.get(i).getName(), valueBuilder.build());
        }
        return rowBuilder.build();
    }

    public QueryResponse executeQuery(Query query, Connection con) {
        switch (query.getQueryCase()) {
            case RAW -> {
                final RawQuery rawQuery = query.getRaw();
                final String sql = queryHelper.getRawQuery(rawQuery);
                // format SQL and log the query

                logger.info("Executing SQL query: {}", field(sql));
                boolean isSelect = sql.trim().toLowerCase().startsWith("select");

                List<Row> rows = null;
                int updateCount = 0;
                if (con != null) {
                    if (isSelect) {
                        rows = jdbcTemplate.query((sql), this::rawQueryMapper, con);
                    } else {
                        updateCount = jdbcTemplate.update((sql), con);
                    }
                } else {
                    if (isSelect) {
                        rows = jdbcTemplate.query((sql), this::rawQueryMapper);
                    } else {
                        updateCount = jdbcTemplate.update((sql));
                    }
                }
                if (isSelect) {
                    return QueryResponse.newBuilder()
                            .setRawResult(RawQueryResult.newBuilder().addAllRows(rows).build())
                            .build();
                } else {
                    return QueryResponse.newBuilder()
                            .setUpdateResult(UpdateQueryResult.newBuilder().setAffectedRows(updateCount).build())
                            .build();
                }
            }
            case SELECT -> {
                final SelectQuery selectQuery = query.getSelect();
                final ParameterizedExpression sql = queryHelper.getSelectQuery(selectQuery);

                logger.info("Executing SQL query: {} with Params: {}", field(sql.getExpression()),
                        Arrays.toString(sql.getParameter()));

                final List<Column> columnList = selectQuery.getColumnList();
                final int numColumns = columnList.size();
                final ColumnType[] columnTypeMap = new ColumnType[numColumns];
                for (int i = 0; i < numColumns; i++) {
                    columnTypeMap[i] = columnList.get(i).getType();
                }
                List<Row> rows;
                if (con != null) {
                    rows = jdbcTemplate.query(sql.getExpression(),
                            (rs, rowNum) -> selectQueryMapper(rs, rowNum, numColumns, columnTypeMap, columnList), con,
                            sql.getParameter());
                } else {
                    rows = jdbcTemplate.query(sql.getExpression(),
                            (rs, rowNum) -> selectQueryMapper(rs, rowNum, numColumns, columnTypeMap, columnList),
                            sql.getParameter());
                }
                return QueryResponse.newBuilder()
                        .setSelectResult(SelectQueryResult.newBuilder().addAllRows(rows).build())
                        .build();
            }
            case INSERT -> {
                final InsertQuery insertQuery = query.getInsert();
                final boolean shouldGenerateId = insertQuery.hasIdSequence()
                        && (insertQuery.hasIdColumn() || insertQuery.hasIdTable());

                final ParameterizedExpression sql;
                long id = 0;
                if (shouldGenerateId) {
                    final String idColumn = insertQuery.getIdColumn();
                    final String idSequence = insertQuery.getIdSequence();
                    id = idSequence.equalsIgnoreCase("MAX")
                            ? idGenerator.getMaxId(insertQuery.getIdTable(), insertQuery.getIdColumn())
                            : idGenerator.getNextId(idSequence);
                    sql = queryHelper.getInsertQuery(insertQuery, idColumn, String.valueOf(id));
                } else {
                    sql = queryHelper.getInsertQuery(insertQuery);
                }

                logger.info("Executing SQL query: {} with Params: {}", field(sql.getExpression()),
                        Arrays.toString(sql.getParameter()));

                if (con != null) {
                    jdbcTemplate.update(sql.getExpression(), con, sql.getParameter());
                } else {
                    jdbcTemplate.update(sql.getExpression(), sql.getParameter());
                }

                InsertQueryResult.Builder insertQueryBuilder = InsertQueryResult.newBuilder();
                if (shouldGenerateId) {
                    insertQueryBuilder.setLastInsertId(id);
                }

                return QueryResponse.newBuilder()
                        .setInsertResult(insertQueryBuilder.build())
                        .build();
            }
            case UPDATE -> {
                final UpdateQuery updateQuery = query.getUpdate();
                final ParameterizedExpression sql = queryHelper.getUpdateQuery(updateQuery);

                logger.info("Executing SQL query: {} with Params: {}", field(sql.getExpression()),
                        Arrays.toString(sql.getParameter()));

                int updateCount = 0;
                if (con != null) {
                    updateCount = jdbcTemplate.update(sql.getExpression(), con, sql.getParameter());
                } else {
                    updateCount = jdbcTemplate.update(sql.getExpression(), sql.getParameter());
                }
                return QueryResponse.newBuilder()
                        .setUpdateResult(UpdateQueryResult.newBuilder().setAffectedRows(updateCount).build())
                        .build();
            }
            case DELETE -> {
                final DeleteQuery deleteQuery = query.getDelete();
                final ParameterizedExpression sql = queryHelper.getDeleteQuery(deleteQuery);

                logger.info("Executing SQL query: {} with Params: {}", field(sql.getExpression()),
                        Arrays.toString(sql.getParameter()));

                int deleteCount = 0;
                if (con != null) {
                    deleteCount = jdbcTemplate.update(sql.getExpression(), con, sql.getParameter());
                } else {
                    deleteCount = jdbcTemplate.update(sql.getExpression(), sql.getParameter());
                }
                return QueryResponse.newBuilder()
                        .setDeleteResult(DeleteQueryResult.newBuilder().setAffectedRows(deleteCount).build())
                        .build();
            }
            case QUERY_NOT_SET ->
                throw new NotImplementedException("Unimplemented case: " + query.getQueryCase());
            default -> throw new IllegalArgumentException("Unexpected value: " + query.getQueryCase());
        }
    }

    @Override
    public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        QueryResponse response = executeQuery(request.getQuery(), null);

        if (response == null) {
            responseObserver.onError(new ExecutionControl.NotImplementedException("Raw query is not implemented"));
            return;
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<QueryRequest> streamQuery(StreamObserver<QueryResponse> responseObserver) {
        return new StreamObserver<>() {
            Connection con = jdbcTemplate.getConnection();
            private final Duration streamTimeout = Duration.ofSeconds(10);
            Duration DEBOUNCE_INTERVAL = Duration.ofMillis(100);
            AtomicLong lastTimerReset = new AtomicLong(System.nanoTime() - DEBOUNCE_INTERVAL.toNanos() - 1);
            private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            AtomicReference<ScheduledFuture<?>> streamTimeoutFuture = new AtomicReference<>(scheduleStreamTimeout());

            @Override
            public void onNext(QueryRequest request) {
                resetStreamTimeout();
                try {
                    QueryResponse response = executeQuery(request.getQuery(), con);
                    responseObserver.onNext(response);
                } catch (Exception e) {
                    rollback();
                    cancelStreamTimeout();
                    throw e;
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.error("Error from client", throwable);
                rollback();
                cancelStreamTimeout();
            }

            @Override
            public void onCompleted() {
                cancelStreamTimeout();
                commit();
                responseObserver.onCompleted();
            }

            private void commit() {
                logger.info("Committing transaction");
                jdbcTemplate.commit(con);
            }

            private void rollback() {
                logger.info("Rolling back transaction");
                jdbcTemplate.rollback(con);
            }

            private synchronized void resetStreamTimeout() {
                if (debounce() && cancelStreamTimeout()) {
                    lastTimerReset.set(System.nanoTime());
                    streamTimeoutFuture.set(scheduleStreamTimeout());
                }
            }

            private boolean debounce() {
                long lastReset = lastTimerReset.get();
                long now = System.nanoTime();
                return (now - lastReset) > DEBOUNCE_INTERVAL.toNanos();
            }

            private boolean cancelStreamTimeout() {
                ScheduledFuture<?> currentFuture = streamTimeoutFuture.get();
                return currentFuture == null || currentFuture.cancel(false);
            }

            private ScheduledFuture<?> scheduleStreamTimeout() {
                return scheduler.schedule(() -> {
                    String message = String.format("RPC timed out after %sms of inactivity",
                            streamTimeout.toMillis());
                    logger.error(message);
                    rollback();
                    cancelStreamTimeout();
                    responseObserver.onCompleted();
                }, streamTimeout.plus(DEBOUNCE_INTERVAL).toNanos(), TimeUnit.NANOSECONDS);
            }
        };
    }
}