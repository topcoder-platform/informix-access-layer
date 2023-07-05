package com.topcoder.dal.util;

import com.topcoder.dal.rdb.*;
import com.topcoder.dal.rdb.Value.ValueCase;

import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

@Component
public class QueryHelper {

    public ParameterizedExpression getSelectQuery(SelectQuery query) {
        final String tableName = query.hasSchema() ? query.getSchema() + ":" + query.getTable() : query.getTable();

        List<Column> columnsList = query.getColumnList();

        final String[] columns = columnsList.stream()
                .map((column -> column.hasTableName() ? column.getTableName() + "." + column.getName()
                        : column.getName()))
                .toArray(String[]::new);

        final List<ParameterizedExpression> whereClause = query.getWhereList().stream()
                .map(toWhereCriteria).toList();

        final Join[] joins = query.getJoinList().toArray(new Join[0]);

        final String[] groupByClause = query.getGroupByList().toArray(new String[0]);
        final String[] orderByClause = query.getOrderByList().toArray(new String[0]);

        final int limit = query.getLimit();
        final int offset = query.getOffset();

        ParameterizedExpression expression = new ParameterizedExpression();
        expression.setExpression("SELECT"
                + (offset > 0 ? " SKIP " + offset : "")
                + (limit > 0 ? " FIRST " + limit : "")
                + (" " + String.join(",", columns) + " FROM " + tableName)
                + (joins.length > 0 ? " " + String.join(" ", Stream.of(joins).map(toJoin).toArray(String[]::new)) : "")
                + (!whereClause.isEmpty()
                        ? " WHERE " + String.join(" AND ",
                                whereClause.stream().map(ParameterizedExpression::getExpression).toArray(String[]::new))
                        : "")
                + (groupByClause.length > 0 ? " GROUP BY " + String.join(",", groupByClause) : "")
                + (orderByClause.length > 0 ? " ORDER BY " + String.join(",", orderByClause) : ""));
        if (!whereClause.isEmpty()) {
            expression.setParameter(
                    whereClause.stream().filter(x -> x.parameter.length > 0).map(x -> x.getParameter()[0]).toArray());
        }
        return expression;
    }

    public ParameterizedExpression getInsertQuery(InsertQuery query) {
        return getInsertQuery(query, null, null);
    }

    public ParameterizedExpression getInsertQuery(InsertQuery query, String idColumn, String idValue) {
        final String tableName = query.hasSchema() ? query.getSchema() + ":" + query.getTable() : query.getTable();
        final List<ColumnValue> valuesToInsert = query.getColumnValueList();

        Stream<String> columnsStream = valuesToInsert.stream()
                .map(ColumnValue::getColumn);

        Stream<Object> paramStream = valuesToInsert.stream()
                .map(ColumnValue::getValue)
                .filter(x -> findSQLExpressionOrFunction(x).isEmpty())
                .map(QueryHelper::toValue);

        Stream<String> valuesStream = valuesToInsert.stream()
                .map(ColumnValue::getValue)
                .map(x -> findSQLExpressionOrFunction(x).orElse("?"));

        final String[] columns;
        final String[] values;
        final Object[] params;

        if (query.hasIdColumn() && query.hasIdSequence()) {
            columns = Stream.concat(Stream.of(idColumn), columnsStream).toArray(String[]::new);
            params = Stream.concat(Stream.of(idValue), paramStream).toArray();
            values = Stream.concat(Stream.of("?"), valuesStream).toArray(String[]::new);
        } else {
            columns = columnsStream.toArray(String[]::new);
            params = paramStream.toArray();
            values = valuesStream.toArray(String[]::new);
        }

        ParameterizedExpression expression = new ParameterizedExpression();
        expression.setExpression("INSERT INTO " + tableName + " (" + String.join(",", columns) + ") VALUES ("
                + String.join(",", values) + ")");
        expression.setParameter(params);
        return expression;
    }

    public ParameterizedExpression getUpdateQuery(UpdateQuery query) {
        final String tableName = query.hasSchema() ? query.getSchema() + ":" + query.getTable() : query.getTable();

        final List<ColumnValue> valuesToUpdate = query.getColumnValueList();
        final String[] columns = valuesToUpdate.stream().map(ColumnValue::getColumn).toArray(String[]::new);

        final String[] values = valuesToUpdate.stream().map(ColumnValue::getValue)
                .map(x -> findSQLExpressionOrFunction(x).orElse("?"))
                .toArray(String[]::new);

        final Stream<Object> paramsStream = valuesToUpdate.stream()
                .map(ColumnValue::getValue)
                .filter(x -> findSQLExpressionOrFunction(x).isEmpty())
                .map(QueryHelper::toValue);


        final List<ParameterizedExpression> whereClause = query.getWhereList().stream()
                .map(toWhereCriteria).toList();

        if (whereClause.isEmpty()) {
            throw new RuntimeException("Update query must have a where clause");
        }
        final Object[] params = Stream
                .concat(paramsStream,
                        whereClause.stream().filter(x -> x.parameter.length > 0).map(x -> x.getParameter()[0]))
                .toArray();

        ParameterizedExpression expression = new ParameterizedExpression();
        expression.setExpression("UPDATE "
                + tableName
                + " SET " + String.join(",", zip(columns, values, (c, v) -> c + "=" + v))
                + " WHERE "
                + String.join(" AND ", whereClause.stream().map(ParameterizedExpression::getExpression).toArray(String[]::new)));
        expression.setParameter(params);
        return expression;
    }

    public ParameterizedExpression getDeleteQuery(DeleteQuery query) {
        final String tableName = query.hasSchema() ? query.getSchema() + ":" + query.getTable() : query.getTable();

        final List<ParameterizedExpression> whereClause = query.getWhereList().stream()
                .map(toWhereCriteria).toList();

        if (whereClause.isEmpty()) {
            throw new IllegalArgumentException("Delete query must have a where clause");
        }
        ParameterizedExpression expression = new ParameterizedExpression();
        expression.setExpression("DELETE FROM "
                + tableName
                + " WHERE "
                + String.join(" AND ", whereClause.stream().map(ParameterizedExpression::getExpression).toArray(String[]::new)));
        expression.setParameter(
                whereClause.stream().filter(x -> x.parameter.length > 0).map(x -> x.getParameter()[0]).toArray());
        return expression;
    }

    public String getRawQuery(RawQuery query) {
        return sanitizeSQLStatement(query.getQuery());
    }

    public static String sanitizeSQLStatement(String sql) {
        if (sql == null || sql.trim().length() == 0) {
            throw new IllegalArgumentException("SQL statement is null or empty");
        }

        // Limit the length of the SQL statement to prevent very long strings
        if (sql.length() > 1000) {
            throw new IllegalArgumentException("SQL statement length exceeds the allowed limit");
        }

        // Whitelist characters
        StringBuilder safeSQL = new StringBuilder();
        for (char c : sql.toCharArray()) {
            if (Character.isLetterOrDigit(c) || c == ' ' || c == ',' || c == '(' || c == ')' || c == '=' || c == '<'
                    || c == '>' || c == '_' || c == ':' || c == '.' || c == '-' || c == '+' || c == '*' || c == '\'') {
                safeSQL.append(c);
            }
        }
        sql = safeSQL.toString();

        // replace single quotes with two single quotes to prevent SQL injection through
        // strings
        sql = sql.replace("'", "''");

        return sql;
    }

    private static final Function<Join, String> toJoin = (join) -> {
        final String joinType = join.getType().toString();
        final String fromTable = join.hasFromTableSchema() ? join.getFromTableSchema() + ":" + join.getFromTable()
                : join.getFromTable();
        final String joinTable = join.hasJoinTableSchema() ? join.getJoinTableSchema() + ":" + join.getJoinTable()
                : join.getJoinTable();
        final String fromColumn = join.getFromColumn();
        final String joinColumn = join.getJoinColumn();

        return joinType + " JOIN " + joinTable + " ON " + joinTable + "." + joinColumn + " = " + fromTable + "."
                + fromColumn;
    };

    private final Function<WhereCriteria, ParameterizedExpression> toWhereCriteria = (criteria) -> {
        String key = criteria.getKey();
        Object value = toValue(criteria.getValue());
        ParameterizedExpression parameterizedExpression = new ParameterizedExpression();

        String clause = switch (criteria.getOperator()) {
            case OPERATOR_EQUAL -> key + " = ?";
            case OPERATOR_NOT_EQUAL -> key + " <> ?";
            case OPERATOR_GREATER_THAN -> key + " > ?";
            case OPERATOR_GREATER_THAN_OR_EQUAL -> key + " >= ?";
            case OPERATOR_LESS_THAN -> key + " < ?";
            case OPERATOR_LESS_THAN_OR_EQUAL -> key + " <= ?";
            case OPERATOR_LIKE -> key + " LIKE ?";
            case OPERATOR_NOT_LIKE -> key + " NOT LIKE ?";
            case OPERATOR_IN -> key + " IN (?)";
            case OPERATOR_NOT_IN -> key + " NOT IN (?)";
            case OPERATOR_IS_NULL -> key + " IS NULL";
            case OPERATOR_IS_NOT_NULL -> key + " IS NOT NULL";
            default -> null;
        };
        Optional<String> foundExpressionOrFunction = findSQLExpressionOrFunction(criteria.getValue());

        if (!criteria.getOperator().equals(Operator.OPERATOR_IS_NULL)
                && !criteria.getOperator().equals(Operator.OPERATOR_IS_NOT_NULL)
                && foundExpressionOrFunction.isPresent()) {
            clause = Objects.requireNonNull(clause).replace("?", foundExpressionOrFunction.get());
        } else if (value != null) {
            parameterizedExpression.setParameter(new Object[] { value });
        }

        parameterizedExpression.setExpression(clause);

        return parameterizedExpression;
    };

    private static Object toValue(Value value) {
        return switch (value.getValueCase()) {
            case STRING_VALUE -> value.getStringValue();
            case INT_VALUE -> value.getIntValue();
            case LONG_VALUE -> value.getLongValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case FLOAT_VALUE -> value.getFloatValue();
            case BOOLEAN_VALUE -> value.getBooleanValue();
            case DATE_VALUE -> value.getDateValue();
            case DATETIME_VALUE -> value.getDatetimeValue();
            case BLOB_VALUE, VALUE_NOT_SET -> null;
        };
    }

    private static Optional<String> findSQLExpressionOrFunction(Value value) {
        List<String> sqlExpressionsAndFunctions = Arrays.asList(
                "CURRENT", "EXTEND", "DATE", "TODAY", "MDY", "YEAR", "MONTH",
                "DAY", "HOUR", "MINUTE", "SECOND"
        );

        if (value.getValueCase().equals(ValueCase.DATE_VALUE)
                || value.getValueCase().equals(ValueCase.DATETIME_VALUE)) {
            String valueStr = value.getValueCase().equals(ValueCase.DATE_VALUE)
                    ? value.getDateValue()
                    : value.getDatetimeValue();

            if (sqlExpressionsAndFunctions.stream().anyMatch(valueStr::contains)) {
                return Optional.of(valueStr);
            }
        }

        return Optional.empty();
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
