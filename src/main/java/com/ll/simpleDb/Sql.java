package com.ll.simpleDb;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Sql {
    private final SimpleDb simpleDb;
    private final StringBuilder sqlBuilder;
    private final List<Object> params;

    public Sql(SimpleDb simpleDb) {
        this.simpleDb = simpleDb;
        this.sqlBuilder = new StringBuilder();
        this.params = new ArrayList<>();
    }

    public Sql append(String sql) {
        if (!sqlBuilder.isEmpty()) {
            sqlBuilder.append(" ");
        }

        sqlBuilder.append(sql);

        return this;
    }

    public Sql append(String sql, Object... args) {
        if (!sqlBuilder.isEmpty()) {
            sqlBuilder.append(" ");
        }

        sqlBuilder.append(sql);

        params.addAll(Arrays.asList(args));

        return this;
    }

    public long insert() {
        String sql = sqlBuilder.toString();

        try (Connection conn = simpleDb.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(i + 1, params.get(i));
            }

            pstmt.executeUpdate();

            try (ResultSet rs = pstmt.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return 0;
    }
}