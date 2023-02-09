package com.topcoder.dal;

import com.topcoder.dal.rdb.*;
import com.topcoder.dal.util.IdGenerator;
import com.topcoder.dal.util.QueryHelper;
import io.grpc.stub.StreamObserver;
import jdk.jshell.spi.ExecutionControl;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;


/**
 * Accessor for rational database like Informix.
 */
@GrpcService
public class DBAccessor extends QueryServiceGrpc.QueryServiceImplBase {

    /**
     * JDBC template.
     */
    private final JdbcTemplate jdbcTemplate;

    private final QueryHelper queryHelper;

    private final IdGenerator idGenerator;

    public DBAccessor(JdbcTemplate jdbcTemplate, QueryHelper queryHelper, IdGenerator idGenerator) {
        this.jdbcTemplate = jdbcTemplate;
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
            result[i] = jdbcTemplate.update(query[i], params == null ? null : (Object[]) params[i]);
        }
        return result;
    }

    // UPDATE X, SET VALUE = NULL;
    // SELECT * FROM X WHERE VALUE = null;

    @Override
    public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
        Query query = request.getQuery();
        QueryResponse response = null;

        switch (query.getQueryCase()) {
            case RAW -> {
                // TODO: Handle Raw
                responseObserver.onError(new ExecutionControl.NotImplementedException("Raw query is not implemented"));
                return;
            }
            case SELECT -> {
                final SelectQuery selectQuery = query.getSelect();
                final String sql = queryHelper.getSelectQuery(selectQuery);

                System.out.println("SQL: " + sql);

                final List<Column> columnList = selectQuery.getColumnList();
                final int numColumns = columnList.size();
                final ColumnType[] columnTypeMap = new ColumnType[numColumns];
                for (int i = 0; i < numColumns; i++) {
                    columnTypeMap[i] = columnList.get(i).getType();
                }
                List<Row> rows = jdbcTemplate.query(sql, (rs, rowNum) -> {
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
                            case COLUMN_TYPE_DATE, COLUMN_TYPE_DATETIME -> valueBuilder.setDateValue(Objects.requireNonNullElse(rs.getTimestamp(i + 1), "").toString());
                            default ->
                                    throw new IllegalArgumentException("Unsupported column type: " + columnTypeMap[i]);
                        }

                        rowBuilder.putValues(columnList.get(i).getName(), valueBuilder.build());
                    }
                    return rowBuilder.build();
                });
                response = QueryResponse.newBuilder()
                        .setSelectResult(SelectQueryResult.newBuilder().addAllRows(rows).build())
                        .build();
            }
            case INSERT -> {
                final InsertQuery insertQuery = query.getInsert();
                final boolean shouldGenerateId = insertQuery.hasIdSequence() && (insertQuery.hasIdColumn() || insertQuery.hasIdTable());

                final String sql;
                long id = 0;
                if (shouldGenerateId) {
                    final String idColumn = insertQuery.getIdColumn();
                    final String idSequence = insertQuery.getIdSequence();
                    id = idSequence.equalsIgnoreCase("MAX") ? idGenerator.getMaxId(insertQuery.getIdTable(), insertQuery.getIdColumn()) : idGenerator.getNextId(idSequence);
                    sql = queryHelper.getInsertQuery(insertQuery, idColumn, String.valueOf(id));
                } else {
                    sql = queryHelper.getInsertQuery(insertQuery);
                }

                System.out.println("SQL: " + sql);
                jdbcTemplate.update(sql);

                InsertQueryResult.Builder insertQueryBuilder = InsertQueryResult.newBuilder();
                if (shouldGenerateId) {
                    insertQueryBuilder.setLastInsertId(id);
                }

                response = QueryResponse.newBuilder()
                        .setInsertResult(insertQueryBuilder.build())
                        .build();
            }
            case UPDATE -> {
                final UpdateQuery updateQuery = query.getUpdate();
                final String sql = queryHelper.getUpdateQuery(updateQuery);
                System.out.println("SQL: " + sql);
                final int updateCount = jdbcTemplate.update(sql);
                response = QueryResponse.newBuilder()
                        .setUpdateResult(UpdateQueryResult.newBuilder().setAffectedRows(updateCount).build())
                        .build();
            }
            case DELETE -> {
                final DeleteQuery deleteQuery = query.getDelete();
                final String sql = queryHelper.getDeleteQuery(deleteQuery);
                final int deleteCount = jdbcTemplate.update(sql);
                response = QueryResponse.newBuilder()
                        .setDeleteResult(DeleteQueryResult.newBuilder().setAffectedRows(deleteCount).build())
                        .build();
            }
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

}
