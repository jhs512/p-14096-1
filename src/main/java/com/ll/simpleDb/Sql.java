package com.ll.simpleDb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class Sql {
    private final SimpleDb simpleDb;
    private final StringBuilder sqlBuilder = new StringBuilder();
    private final List<Object> params = new ArrayList<>();
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .setPropertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE);

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

    public Sql appendIn(String sql, Object... args) {
        if (!sqlBuilder.isEmpty()) {
            sqlBuilder.append(" ");
        }

        String placeholders = String.join(", ", Collections.nCopies(args.length, "?"));
        sqlBuilder.append(sql.replace("?", placeholders));

        params.addAll(Arrays.asList(args));

        return this;
    }

    public Sql execute() {
        String sql = getSqlString();
        logSqlIfDevMode(sql);

        Connection conn = null;
        try {
            conn = simpleDb.getConnection();
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                bindParameters(pstmt);
                pstmt.execute();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeConnectionIfNotInTransaction(conn);
        }

        return this;
    }

    public long insert() {
        String sql = getSqlString();
        logSqlIfDevMode(sql);

        Connection conn = null;
        try {
            conn = simpleDb.getConnection();
            try (PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                bindParameters(pstmt);
                pstmt.executeUpdate();

                try (ResultSet rs = pstmt.getGeneratedKeys()) {
                    if (rs.next()) {
                        return rs.getLong(1);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeConnectionIfNotInTransaction(conn);
        }

        return 0;
    }

    public int update() {
        return executeUpdate();
    }

    public int delete() {
        return executeUpdate();
    }

    private int executeUpdate() {
        String sql = getSqlString();
        logSqlIfDevMode(sql);

        Connection conn = null;
        try {
            conn = simpleDb.getConnection();
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                bindParameters(pstmt);
                return pstmt.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeConnectionIfNotInTransaction(conn);
        }
    }

    private String getSqlString() {
        return sqlBuilder.toString();
    }

    private void logSqlIfDevMode(String sql) {
        if (simpleDb.isDevMode()) {
            System.out.println("SQL: " + sql);
        }
    }

    public List<Map<String, Object>> selectRows() {
        String sql = getSqlString();
        logSqlIfDevMode(sql);

        Connection conn = null;
        try {
            conn = simpleDb.getConnection();
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                bindParameters(pstmt);

                try (ResultSet rs = pstmt.executeQuery()) {
                    return resultSetToMapList(rs);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeConnectionIfNotInTransaction(conn);
        }
    }

    public Map<String, Object> selectRow() {
        List<Map<String, Object>> rows = selectRows();
        return rows.isEmpty() ? null : rows.get(0);
    }

    public Long selectLong() {
        String sql = getSqlString();
        logSqlIfDevMode(sql);

        Connection conn = null;
        try {
            conn = simpleDb.getConnection();
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                bindParameters(pstmt);

                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        return rs.getLong(1);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeConnectionIfNotInTransaction(conn);
        }

        return null;
    }

    public String selectString() {
        String sql = getSqlString();
        logSqlIfDevMode(sql);

        Connection conn = null;
        try {
            conn = simpleDb.getConnection();
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                bindParameters(pstmt);

                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeConnectionIfNotInTransaction(conn);
        }

        return null;
    }

    public Boolean selectBoolean() {
        String sql = getSqlString();
        logSqlIfDevMode(sql);

        Connection conn = null;
        try {
            conn = simpleDb.getConnection();
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                bindParameters(pstmt);

                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        Object value = rs.getObject(1);
                        if (value instanceof Boolean) {
                            return (Boolean) value;
                        }
                        if (value instanceof Number) {
                            return ((Number) value).intValue() != 0;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeConnectionIfNotInTransaction(conn);
        }

        return null;
    }

    public LocalDateTime selectDatetime() {
        String sql = getSqlString();
        logSqlIfDevMode(sql);

        Connection conn = null;
        try {
            conn = simpleDb.getConnection();
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                bindParameters(pstmt);

                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        return rs.getObject(1, LocalDateTime.class);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeConnectionIfNotInTransaction(conn);
        }

        return null;
    }

    public List<Long> selectLongs() {
        String sql = getSqlString();
        logSqlIfDevMode(sql);

        Connection conn = null;
        try {
            conn = simpleDb.getConnection();
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                bindParameters(pstmt);

                try (ResultSet rs = pstmt.executeQuery()) {
                    List<Long> results = new ArrayList<>();
                    while (rs.next()) {
                        results.add(rs.getLong(1));
                    }
                    return results;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            closeConnectionIfNotInTransaction(conn);
        }
    }

    public <T> List<T> selectRows(Class<T> clazz) {
        List<Map<String, Object>> rows = selectRows();
        return rows.stream()
                .map(row -> objectMapper.convertValue(row, clazz))
                .collect(Collectors.toList());
    }

    public <T> T selectRow(Class<T> clazz) {
        Map<String, Object> row = selectRow();
        return row == null ? null : objectMapper.convertValue(row, clazz);
    }

    private List<Map<String, Object>> resultSetToMapList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                Object value = rs.getObject(i);

                if (value instanceof java.sql.Timestamp) {
                    value = ((java.sql.Timestamp) value).toLocalDateTime();
                } else if (value instanceof Boolean || (value != null && value.getClass().getName().equals("java.lang.Boolean"))) {
                    value = (Boolean) value;
                } else if (value != null && value.getClass().getSimpleName().equals("Byte")) {
                    value = ((Number) value).intValue() != 0;
                }

                row.put(columnName, value);
            }
            rows.add(row);
        }

        return rows;
    }

    private void bindParameters(PreparedStatement pstmt) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            pstmt.setObject(i + 1, params.get(i));
        }
    }

    private void closeConnectionIfNotInTransaction(Connection conn) {
        if (conn != null && !simpleDb.isInTransaction()) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}