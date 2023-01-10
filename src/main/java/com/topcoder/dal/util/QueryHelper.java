package com.topcoder.dal.util;

import com.topcoder.dal.rdb.*;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

@Component
public class QueryHelper {

    public String getSelectQuery(SelectQuery query) {
        final String tableName = query.hasSchema() ? query.getSchema() + ":" + query.getTable() : query.getTable();

        List<Column> columnsList = query.getColumnList();

        final String[] columns = columnsList.stream()
                .map((column -> column.hasTableName() ? column.getTableName() + "." + column.getName() : column.getName()))
                .toArray(String[]::new);

        final String[] whereClause = query.getWhereList().stream()
                .map(toWhereCriteria)
                .toArray(String[]::new);

        final Join[] joins = query.getJoinList().toArray(new Join[0]);

        final String[] groupByClause = query.getGroupByList().toArray(new String[0]);
        final String[] orderByClause = query.getOrderByList().toArray(new String[0]);

        final int limit = query.getLimit();
        final int offset = query.getOffset();

        return "SELECT"
                + (offset > 0 ? " SKIP " + offset : "")
                + (limit > 0 ? " FIRST " + limit : "")
                + (" " + String.join(",", columns) + " FROM " + tableName)
                + (joins.length > 0 ? " " + String.join(" ", Stream.of(joins).map(toJoin).toArray(String[]::new)) : "")
                + (whereClause.length > 0 ? " WHERE " + String.join(" AND ", whereClause) : "")
                + (groupByClause.length > 0 ? " GROUP BY " + String.join(",", groupByClause) : "")
                + (orderByClause.length > 0 ? " ORDER BY " + String.join(",", orderByClause) : "");
    }


    public String getInsertQuery(InsertQuery query) {
        return getInsertQuery(query, null, null);
    }

    public String getInsertQuery(InsertQuery query, String idColumn, String idValue) {
        final String tableName = query.hasSchema() ? query.getSchema() + ":" + query.getTable() : query.getTable();
        final List<ColumnValue> valuesToInsert = query.getColumnValueList();

        Stream<String> columnsStream = valuesToInsert.stream()
                .map(ColumnValue::getColumn);

        Stream<String> valueStream = valuesToInsert.stream()
                .map(ColumnValue::getValue)
                .map(QueryHelper::toValue);

        final String[] columns;
        final String[] values;

        if (query.hasIdColumn() && query.hasIdSequence()) {
            columns = Stream.concat(Stream.of(idColumn), columnsStream).toArray(String[]::new);
            values = Stream.concat(Stream.of(idValue), valueStream).toArray(String[]::new);
        } else {
            columns = columnsStream.toArray(String[]::new);
            values = valueStream.toArray(String[]::new);
        }

        return "INSERT INTO " + tableName + " (" + String.join(",", columns) + ") VALUES (" + String.join(",", values) + ")";
    }


    public String getUpdateQuery(UpdateQuery query) {
        final String tableName = query.hasSchema() ? query.getSchema() + ":" + query.getTable() : query.getTable();

        final List<ColumnValue> valuesToUpdate = query.getColumnValueList();
        final String[] columns = valuesToUpdate.stream().map(ColumnValue::getColumn).toArray(String[]::new);
        final String[] values = valuesToUpdate.stream().map(ColumnValue::getValue).map(QueryHelper::toValue).toArray(String[]::new);

        final String[] whereClause = query.getWhereList().stream()
                .map(toWhereCriteria)
                .toArray(String[]::new);

        if (whereClause.length == 0) {
            throw new RuntimeException("Update query must have a where clause");
        }

        return "UPDATE "
                + tableName
                + " SET " + String.join(",", zip(columns, values, (c, v) -> c + "=" + "'" + v + "'"))
                + " WHERE " + String.join(" AND ", whereClause);
    }

    public String getDeleteQuery(DeleteQuery query) {
        final String tableName = query.hasSchema() ? query.getSchema() + ":" + query.getTable() : query.getTable();

        final String[] whereClause = query.getWhereList().stream()
                .map(toWhereCriteria)
                .toArray(String[]::new);

        if (whereClause.length == 0) {
            throw new IllegalArgumentException("Delete query must have a where clause");
        }

        return "DELETE FROM "
                + tableName
                + " WHERE " + String.join(" AND ", whereClause);
    }

    private static final Function<Join, String> toJoin = (join) -> {
        final String joinType = join.getType().toString();
        final String fromTable = join.hasFromTableSchema() ? join.getFromTableSchema() + ":" + join.getFromTable() : join.getFromTable();
        final String joinTable = join.hasJoinTableSchema() ? join.getJoinTableSchema() + ":" + join.getJoinTable() : join.getJoinTable();
        final String fromColumn = join.getFromColumn();
        final String joinColumn = join.getJoinColumn();

        return joinType + " JOIN " + joinTable + " ON " + joinTable + "." + joinColumn + " = " + fromTable + "." + fromColumn;
    };

    private static final Function<WhereCriteria, String> toWhereCriteria = (criteria) -> {
        String key = criteria.getKey();
        String value = toValue(criteria.getValue());

        return switch (criteria.getOperator()) {
            case EQUAL -> key + "=" + value;
            case NOT_EQUAL -> key + "<>" + value;
            case GREATER_THAN -> key + ">" + value;
            case GREATER_THAN_OR_EQUAL -> key + ">=" + value;
            case LESS_THAN -> key + "<" + value;
            case LESS_THAN_OR_EQUAL -> key + "<=" + value;
            case LIKE -> key + " LIKE " + value;
            case NOT_LIKE -> key + " NOT LIKE " + value;
            case IN -> key + " IN (" + value + ")";
            case NOT_IN -> key + " NOT IN (" + value + ")";
            case IS_NULL -> key + " IS NULL";
            case IS_NOT_NULL -> key + " IS NOT NULL";
            default -> null;
        };
    };


    private static String toValue(Value value) {
        return switch (value.getValueCase()) {
            case STRING_VALUE -> "'" + value.getStringValue() + "'";
            case INT_VALUE -> String.valueOf(value.getIntValue());
            case LONG_VALUE -> String.valueOf(value.getLongValue());
            case DOUBLE_VALUE -> String.valueOf(value.getDoubleValue());
            case FLOAT_VALUE -> String.valueOf(value.getFloatValue());
            case BOOLEAN_VALUE -> String.valueOf(value.getBooleanValue());
            case DATE_VALUE -> value.getDateValue();
            case DATETIME_VALUE -> value.getDatetimeValue();
            case BLOB_VALUE, VALUE_NOT_SET -> null;
        };
    }

    private static String[] zip(String[] columns, String[] values, BiFunction<String, String, String> f) {
        final int length = columns.length;
        final String[] result = new String[length];

        for (int i = 0; i < length; i++) {
            result[i] = f.apply(columns[i], values[i]);
        }

        return result;
    }
}
