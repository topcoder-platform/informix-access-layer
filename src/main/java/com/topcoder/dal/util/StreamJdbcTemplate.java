package com.topcoder.dal.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.core.SqlProvider;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

public class StreamJdbcTemplate extends JdbcTemplate {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public StreamJdbcTemplate(DataSource dataSource) {
        super(dataSource);
    }

    public <T> List<T> query(String sql, RowMapper<T> rowMapper, Connection con) throws DataAccessException {
        return result(query(sql, new RowMapperResultSetExtractor<>(rowMapper), con));
    }

    @Nullable
    public <T> T query(final String sql, final ResultSetExtractor<T> rse, Connection con) throws DataAccessException {
        Assert.notNull(sql, "SQL must not be null");
        Assert.notNull(rse, "ResultSetExtractor must not be null");

        class QueryStatementCallback implements StatementCallback<T>, SqlProvider {
            @Override
            @Nullable
            public T doInStatement(Statement stmt) throws SQLException {
                ResultSet rs = null;
                try {
                    rs = stmt.executeQuery(sql);
                    return rse.extractData(rs);
                } finally {
                    closeResultSet(rs);
                }
            }

            @Override
            public String getSql() {
                return sql;
            }
        }

        return execute(new QueryStatementCallback(), con);
    }

    public int update(final String sql, Connection con) throws DataAccessException {
        Assert.notNull(sql, "SQL must not be null");

        class UpdateStatementCallback implements StatementCallback<Integer>, SqlProvider {
            @Override
            public Integer doInStatement(Statement stmt) throws SQLException {
                int rows = stmt.executeUpdate(sql);
                if (logger.isTraceEnabled()) {
                    logger.trace("SQL update affected " + rows + " rows");
                }
                return rows;
            }

            @Override
            public String getSql() {
                return sql;
            }
        }

        return updateCount(execute(new UpdateStatementCallback(), con));
    }

    @Nullable
    private <T> T execute(StatementCallback<T> action, Connection con) throws DataAccessException {
        Assert.notNull(action, "Callback object must not be null");
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            applyStatementSettings(stmt);
            T result = action.doInStatement(stmt);
            handleWarnings(stmt);
            return result;
        } catch (SQLException ex) {
            String sql = getSql(action);
            closeStatement(stmt);
            throw translateException("StatementCallback", sql, ex);
        } finally {
            closeStatement(stmt);
        }
    }

    @Nullable
    private static String getSql(Object sqlProvider) {
        if (sqlProvider instanceof SqlProvider) {
            return ((SqlProvider) sqlProvider).getSql();
        } else {
            return null;
        }
    }

    private static int updateCount(@Nullable Integer result) {
        Assert.state(result != null, "No update count");
        return result;
    }

    private static <T> T result(@Nullable T result) {
        Assert.state(result != null, "No result");
        return result;
    }

    public Connection getConnection() throws CannotGetJdbcConnectionException {
        try {
            Connection con = getDataSource().getConnection();
            if (con == null) {
                throw new IllegalStateException("DataSource returned null from getConnection(): " + getDataSource());
            }
            con.setAutoCommit(false);
            return con;
        } catch (SQLException ex) {
            throw new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection", ex);
        } catch (IllegalStateException ex) {
            throw new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection", ex);
        }
    }

    public void closeConnection(@Nullable Connection con) {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException ex) {
                logger.error("Could not close JDBC Connection", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on closing JDBC Connection", ex);
            }
        }
    }

    public void closeStatement(@Nullable Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
                stmt = null;
            } catch (SQLException ex) {
                logger.error("Could not close JDBC Statement", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on closing JDBC Statement", ex);
            }
        }
    }

    public void closeResultSet(@Nullable ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ex) {
                logger.error("Could not close JDBC ResultSet", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on closing JDBC ResultSet", ex);
            }
        }
    }

    public void setTransactionIsolation(Connection con, int transaction) {
        if (con != null) {
            try {
                con.setTransactionIsolation(transaction);
            } catch (SQLException ex) {
                logger.error("Could not set transaction level", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on setting transaction level", ex);
            }
        }
    }

    public void commit(Connection con) {
        if (con != null) {
            try {
                con.commit();
                closeConnection(con);
                con = null;
            } catch (SQLException ex) {
                logger.error("Could not commit", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on commit", ex);
            }
        }
    }

    public void rollback(Connection con) {
        if (con != null) {
            try {
                con.rollback();
                closeConnection(con);
                con = null;
            } catch (SQLException ex) {
                logger.error("Could not rollback", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on rollback", ex);
            }
        }
    }
}
