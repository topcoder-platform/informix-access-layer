package com.topcoder.dal.util;

import com.topcoder.dal.rdb.*;
import com.topcoder.dal.rdb.Value.ValueCase;

import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

@Component
public class QueryHelper {
    private final AtomicInteger aliasCounter = new AtomicInteger(0);

    private String generateAlias(String fullTableName) {
        String[] parts = fullTableName.split(":");
        String tableName = parts.length > 1 ? parts[1] : parts[0];
        String prefix = tableName.substring(0, 1).toLowerCase();
        return prefix + aliasCounter.incrementAndGet();
    }


    public ParameterizedExpression getSelectQuery(SelectQuery query) {
        aliasCounter.set(0);

        final HashMap<String, String> tableAliases = new HashMap<>();
        final String primaryTableName = query.hasSchema() ? query.getSchema() + ":" + query.getTable() : query.getTable();
        final String tableAlias = generateAlias(primaryTableName);
        tableAliases.put(primaryTableName, tableAlias);

        final String table = (query.hasSchema() ? query.getSchema() + ":" + query.getTable() : query.getTable()) + " " + tableAlias;
        List<Column> columnsList = query.getColumnList();

        final Join[] joins = query.getJoinList().toArray(new Join[0]);
        for (Join join : joins) {
            tableAliases.computeIfAbsent(join.hasFromTableSchema() ? join.getFromTableSchema() + ":" + join.getFromTable() : join.getFromTable(), this::generateAlias);
            tableAliases.computeIfAbsent(join.hasJoinTableSchema() ? join.getJoinTableSchema() + ":" + join.getJoinTable() : join.getJoinTable(), this::generateAlias);
        }

        final String[] columns = columnsList.stream()
                .map(column -> column.hasTableName()
                        ? tableAliases.getOrDefault(column.getTableName(), tableAlias) + "." + column.getName() // TODO: This keeps current systems working correctly. Plan to update proto definition of ColumnValue to include schema
                        : tableAlias + "." + column.getName())
                .toArray(String[]::new);

        final List<ParameterizedExpression> whereClause = query.getWhereList().stream()
                .map((criteria) -> {
                    ParameterizedExpression parameterizedExpression = toWhereCriteria.apply(criteria);
                    final String key = criteria.getKey();
                    final String[] parts = key.split("\\.");
                    if (parts.length > 1) {
                        parameterizedExpression.setExpression(parameterizedExpression.getExpression().replace(key, tableAliases.get(parts[0]) + "." + parts[1]));
                    } else {
                        parameterizedExpression.setExpression(parameterizedExpression.getExpression().replace(key, tableAlias + "." + key));
                    }
                    return parameterizedExpression;
                })
                .toList();

        final String[] groupByClause = query.getGroupByList().toArray(new String[0]);
        final String[] orderByClause = query.getOrderByList().toArray(new String[0]);

        final int limit = query.getLimit();
        final int offset = query.getOffset();

        ParameterizedExpression expression = new ParameterizedExpression();
        expression.setExpression("SELECT"
                + (offset > 0 ? " SKIP " + offset : "")
                + (limit > 0 ? " FIRST " + limit : "")
                + (" " + String.join(",", columns) + " FROM " + table)
                + (joins.length > 0 ? " " + String.join(" ", Stream.of(joins).map(join -> toJoin(join, tableAliases)).toArray(String[]::new)) : "")
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

    public String toJoin(Join join, HashMap<String, String> tableAliases) {
        final String joinType = join.getType().toString().replace("JOIN_TYPE_", "");

        final String joinTableName = join.hasJoinTableSchema() ? join.getJoinTableSchema() + ":" + join.getJoinTable() : join.getJoinTable();
        final String fromTableName = join.hasFromTableSchema() ? join.getFromTableSchema() + ":" + join.getFromTable() : join.getFromTable();

        final String joinTableAlias = tableAliases.getOrDefault(joinTableName, generateAlias(joinTableName));
        final String fromTableAlias = tableAliases.getOrDefault(fromTableName, generateAlias(fromTableName));

        final String fromColumn = join.getFromColumn();
        final String joinColumn = join.getJoinColumn();

        return joinType + " JOIN " + joinTableName + " AS " + joinTableAlias + " ON " + joinTableAlias + "." + joinColumn + " = " + fromTableAlias + "." + fromColumn;
    }

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
