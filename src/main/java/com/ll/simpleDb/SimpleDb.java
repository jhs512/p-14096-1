package com.ll.simpleDb;

import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SimpleDb {
    private final String url;
    private final String username;
    private final String password;
    private final ThreadLocal<Connection> transactionConnection = new ThreadLocal<>();

    @Getter
    @Setter
    private boolean devMode;

    public SimpleDb(String host, String username, String password, String dbName) {
        this.url = String.format("jdbc:mysql://%s:3306/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Seoul", host, dbName);
        this.username = username;
        this.password = password;
    }

    public Sql genSql() {
        return new Sql(this);
    }

    Connection getConnection() throws SQLException {
        Connection conn = transactionConnection.get();
        if (conn != null) {
            return conn;
        }
        return DriverManager.getConnection(url, username, password);
    }

    boolean isInTransaction() {
        return transactionConnection.get() != null;
    }

    public void run(String sql, Object... args) {
        genSql()
                .append(sql, args)
                .execute();
    }

    public void startTransaction() {
        try {
            Connection conn = DriverManager.getConnection(url, username, password);
            conn.setAutoCommit(false);
            transactionConnection.set(conn);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void commit() {
        Connection conn = transactionConnection.get();
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                closeTransactionConnection();
            }
        }
    }

    public void rollback() {
        Connection conn = transactionConnection.get();
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                closeTransactionConnection();
            }
        }
    }

    public void close() {
        closeTransactionConnection();
    }

    private void closeTransactionConnection() {
        Connection conn = transactionConnection.get();
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                transactionConnection.remove();
            }
        }
    }
}