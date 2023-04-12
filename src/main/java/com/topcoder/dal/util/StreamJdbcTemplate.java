package com.topcoder.dal.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
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
import org.springframework.jdbc.core.ParameterDisposer;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.PreparedStatementSetter;
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

    public <T> List<T> query(String sql, RowMapper<T> rowMapper, Connection con, @Nullable Object... args)
            throws DataAccessException {
        return result(query(sql, args, new RowMapperResultSetExtractor<>(rowMapper), con));
    }

    @Nullable
    public <T> T query(String sql, @Nullable Object[] args, ResultSetExtractor<T> rse, Connection con)
            throws DataAccessException {
        return query(sql, newArgPreparedStatementSetter(args), rse, con);
    }

    @Nullable
    public <T> T query(String sql, @Nullable PreparedStatementSetter pss, ResultSetExtractor<T> rse, Connection con)
            throws DataAccessException {
        return query(new SimplePreparedStatementCreator(sql), pss, rse, con);
    }

    @Nullable
    public <T> T query(
            PreparedStatementCreator psc, @Nullable final PreparedStatementSetter pss, final ResultSetExtractor<T> rse,
            Connection con)
            throws DataAccessException {

        Assert.notNull(rse, "ResultSetExtractor must not be null");

        return execute(psc, new PreparedStatementCallback<T>() {
            @Override
            @Nullable
            public T doInPreparedStatement(PreparedStatement ps) throws SQLException {
                ResultSet rs = null;
                try {
                    if (pss != null) {
                        pss.setValues(ps);
                    }
                    rs = ps.executeQuery();
                    return rse.extractData(rs);
                } finally {
                    closeResultSet(rs);
                    if (pss instanceof ParameterDisposer) {
                        ((ParameterDisposer) pss).cleanupParameters();
                    }
                }
            }
        }, con);
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

    public int update(String sql, Connection con, @Nullable Object... args) throws DataAccessException {
        return update(sql, newArgPreparedStatementSetter(args), con);
    }

    public int update(String sql, @Nullable PreparedStatementSetter pss, Connection con) throws DataAccessException {
        return update(new SimplePreparedStatementCreator(sql), pss, con);
    }

    private int update(final PreparedStatementCreator psc, @Nullable final PreparedStatementSetter pss, Connection con)
            throws DataAccessException {

        return updateCount(execute(psc, ps -> {
            try {
                if (pss != null) {
                    pss.setValues(ps);
                }
                int rows = ps.executeUpdate();
                return rows;
            } finally {
                if (pss instanceof ParameterDisposer) {
                    ((ParameterDisposer) pss).cleanupParameters();
                }
            }
        }, con));
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
    private <T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action, Connection con)
            throws DataAccessException {

        Assert.notNull(psc, "PreparedStatementCreator must not be null");
        Assert.notNull(action, "Callback object must not be null");

        PreparedStatement ps = null;
        try {
            ps = psc.createPreparedStatement(con);
            applyStatementSettings(ps);
            T result = action.doInPreparedStatement(ps);
            handleWarnings(ps);
            return result;
        } catch (SQLException ex) {
            if (psc instanceof ParameterDisposer) {
                ((ParameterDisposer) psc).cleanupParameters();
            }
            String sql = getSql(psc);
            psc = null;
            closeStatement(ps);
            ps = null;
            throw translateException("PreparedStatementCallback", sql, ex);
        } finally {
            if (psc instanceof ParameterDisposer) {
                ((ParameterDisposer) psc).cleanupParameters();
            }
            closeStatement(ps);
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

    private static class SimplePreparedStatementCreator implements PreparedStatementCreator, SqlProvider {

        private final String sql;

        public SimplePreparedStatementCreator(String sql) {
            Assert.notNull(sql, "SQL must not be null");
            this.sql = sql;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
            return con.prepareStatement(this.sql);
        }

        @Override
        public String getSql() {
            return this.sql;
        }
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
