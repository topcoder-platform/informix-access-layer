package com.topcoder.dal.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;

@Component
public class IdGenerator {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final HashMap<String, long[]> sequenceNames = new HashMap<>();

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public long getNextId(final String sequenceName) {
        long availableId = 0;
        long nextId = 0;

        if (!sequenceNames.containsKey(sequenceName)) {
            sequenceNames.put(sequenceName, new long[] { availableId, nextId });
        }

        final long[] ids = sequenceNames.get(sequenceName);
        availableId = ids[0];
        nextId = ids[1];
        availableId--;

        if (availableId <= 0) {
            final long[] nextBlock = getNextBlock(sequenceName);

            if (nextBlock == null) {
                throw new RuntimeException("Unable to get next block");
            }

            nextId = nextBlock[0];
            availableId = nextBlock[1];

            if (!updateNextBlock(sequenceName, nextId + availableId + 1)) {
                throw new RuntimeException("Unable to update next block");
            }
        }

        nextId++;

        sequenceNames.put(sequenceName, new long[] { availableId, nextId });

        return nextId;
    }

    private long[] getNextBlock(final String sequenceName) {
        String sql = "SELECT next_block_start, block_size from id_sequences where name = '" + sequenceName + "'";
        System.out.println("SQL: " + sql);

        return jdbcTemplate.query(sql, rs -> {
            long[] ret = { 0, 0 };
            if (rs.next()) {
                long nextBlockStart = rs.getLong("next_block_start");
                int blockSize = rs.getInt("block_size");

                ret[0] = nextBlockStart - 1;
                ret[1] = blockSize;

                return ret;
            }
            return null;
        });
    }

    public boolean updateNextBlock(final String sequenceName, final long nextStart) {
        String sql = "UPDATE id_sequences SET next_block_start = " + nextStart + " WHERE name = '" + sequenceName + "'";
        System.out.println("SQL: " + sql);
        return jdbcTemplate.update(sql) == 1;
    }

    public long getMaxId(final String tableName, final String idColumn) {
        String sql = "SELECT MAX(" + idColumn + ") + 1 as id FROM " + tableName;
        System.out.println("SQL: " + sql);

        var maxId = jdbcTemplate.query(sql, rs -> {
            if (rs.next()) {
                return rs.getLong("id");
            }

            return 0;
        });

        assert maxId != null;
        return maxId.longValue();
    }
}
