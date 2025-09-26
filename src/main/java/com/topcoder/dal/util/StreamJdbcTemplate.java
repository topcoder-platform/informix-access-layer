package com.topcoder.dal.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;

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

    private static final int DEFAULT_LOCK_MODE_WAIT_SECONDS = 10;
    private static final int LOCK_MODE_WAIT_INCREMENT_SECONDS = 10;
    private static final int LOCK_MODE_WAIT_MAX_SECONDS = 30;
    private static final String ISOLATION_SQL = "SET ISOLATION TO COMMITTED READ LAST COMMITTED";
    private static final int INFORMIX_POSITION_ERROR = -243;
    private static final int INFORMIX_LOCK_TIMEOUT_ERROR = -143;
    private static final String INFORMIX_SQL_STATE = "IX000";
    private static final int LOCK_TIMEOUT_MAX_RETRIES = 3;
    private static final long LOCK_TIMEOUT_RETRY_BACKOFF_MS = 200L;

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private final ThreadLocal<Integer> lockModeWaitOverride = new ThreadLocal<>();

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
        return withLockTimeoutRetry(con, sql,
                () -> query(sql, newArgPreparedStatementSetter(args), rse, con));
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
        applyReadSessionSettings(con);

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
        return withLockTimeoutRetry(con, sql, () -> queryInternal(sql, rse, con));
    }

    private <T> T queryInternal(final String sql, final ResultSetExtractor<T> rse, Connection con)
            throws DataAccessException {
        Assert.notNull(sql, "SQL must not be null");
        Assert.notNull(rse, "ResultSetExtractor must not be null");

        applyReadSessionSettings(con);

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
        applyLockModeWaitSetting(con);

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

        applyLockModeWaitSetting(con);

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

    @FunctionalInterface
    private interface DataAccessCallback<T> {
        T doWithDataAccess() throws DataAccessException;
    }

    private <T> T withLockTimeoutRetry(@Nullable Connection con, @Nullable String sql, DataAccessCallback<T> action)
            throws DataAccessException {
        int attempt = 0;
        Integer previousOverride = lockModeWaitOverride.get();
        boolean lockWaitAdjusted = false;
        try {
            while (true) {
                try {
                    return action.doWithDataAccess();
                } catch (DataAccessException ex) {
                    if (!isInformixLockTimeout(ex) || attempt >= LOCK_TIMEOUT_MAX_RETRIES) {
                        throw ex;
                    }
                    attempt++;
                    long delay = LOCK_TIMEOUT_RETRY_BACKOFF_MS * attempt;
                    Throwable cause = ex.getMostSpecificCause();
                    String causeMessage = cause != null ? cause.getMessage() : ex.getMessage();
                    String sqlForLog = sql != null ? sql : "<unknown>";
                    String waitDescription = "unchanged (connection not supplied)";
                    if (con != null) {
                        int waitSeconds = computeLockModeWaitSeconds(attempt);
                        setLockModeWaitOverride(waitSeconds);
                        applyLockModeWaitSetting(con);
                        waitDescription = describeLockModeWait(waitSeconds);
                        lockWaitAdjusted = true;
                    }
                    logger.warn(
                            "Informix lock timeout detected for SQL [{}] (attempt {}/{}). "
                                    + "Increasing lock wait to {} and retrying after {} ms. Cause: {}",
                            sqlForLog,
                            attempt,
                            LOCK_TIMEOUT_MAX_RETRIES,
                            waitDescription,
                            delay,
                            causeMessage);
                    if (delay > 0) {
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw ex;
                        }
                    }
                }
            }
        } finally {
            if (previousOverride == null) {
                setLockModeWaitOverride(null);
            } else {
                setLockModeWaitOverride(previousOverride);
            }
            if (con != null && lockWaitAdjusted) {
                applyLockModeWaitSetting(con);
            }
        }
    }

    private void setLockModeWaitOverride(@Nullable Integer waitSeconds) {
        if (waitSeconds == null) {
            lockModeWaitOverride.remove();
        } else {
            lockModeWaitOverride.set(waitSeconds);
        }
    }

    private String describeLockModeWait(int waitSeconds) {
        return waitSeconds < 0 ? "indefinite" : waitSeconds + "s";
    }

    private int computeLockModeWaitSeconds(int attempt) {
        long candidate = (long) DEFAULT_LOCK_MODE_WAIT_SECONDS
                + (long) attempt * LOCK_MODE_WAIT_INCREMENT_SECONDS;
        if (LOCK_MODE_WAIT_MAX_SECONDS > 0 && candidate > LOCK_MODE_WAIT_MAX_SECONDS) {
            return -1;
        }
        return (int) Math.min(candidate, Integer.MAX_VALUE);
    }

    private String resolveLockModeWaitSql() {
        Integer override = lockModeWaitOverride.get();
        int waitSeconds = override != null ? override : DEFAULT_LOCK_MODE_WAIT_SECONDS;
        return lockModeWaitSql(waitSeconds);
    }

    private static String lockModeWaitSql(int waitSeconds) {
        return waitSeconds <= 0 ? "SET LOCK MODE TO WAIT" : "SET LOCK MODE TO WAIT " + waitSeconds;
    }

    private boolean isInformixLockTimeout(@Nullable Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLException sqlEx) {
                int errorCode = sqlEx.getErrorCode();
                if (errorCode == INFORMIX_POSITION_ERROR || errorCode == INFORMIX_LOCK_TIMEOUT_ERROR) {
                    return true;
                }
                String sqlState = sqlEx.getSQLState();
                if (sqlState != null && INFORMIX_SQL_STATE.equals(sqlState)) {
                    String message = sqlEx.getMessage();
                    if (message != null && message.toLowerCase(Locale.ROOT).contains("lock timeout")) {
                        return true;
                    }
                }
            }
            current = current.getCause();
        }
        return false;
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
        Connection con = null;
        try {
            con = getDataSource().getConnection();
            if (con == null) {
                throw new IllegalStateException("DataSource returned null from getConnection(): " + getDataSource());
            }
            con.setAutoCommit(false);
            logger.info("Opened JDBC connection; applying USELASTCOMMITTED=ALL");
            try (Statement stmt = con.createStatement()) {
                stmt.execute("SET ENVIRONMENT USELASTCOMMITTED 'ALL'");
                logger.info("Applied USELASTCOMMITTED=ALL on connection");
            } catch (SQLException ex) {
                logger.warn("Could not set USELASTCOMMITTED environment", ex);
            }
            return con;
        } catch (SQLException ex) {
            closeConnection(con);
            throw new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection", ex);
        } catch (IllegalStateException ex) {
            closeConnection(con);
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

    private void applyReadSessionSettings(@Nullable Connection con) {
        if (con == null) {
            return;
        }
        applyLockModeWaitSetting(con);
        try (Statement stmt = con.createStatement()) {
            stmt.execute(ISOLATION_SQL);
        } catch (SQLException ex) {
            logger.warn("Could not apply read isolation setting", ex);
        }
    }

    private void applyLockModeWaitSetting(@Nullable Connection con) {
        if (con == null) {
            return;
        }
        try (Statement stmt = con.createStatement()) {
            stmt.execute(resolveLockModeWaitSql());
        } catch (SQLException ex) {
            logger.warn("Could not apply lock wait setting", ex);
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
