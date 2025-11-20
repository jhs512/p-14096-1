package com.ll.simpleDb;

import lombok.Setter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SimpleDb {
    private final String url;
    private final String username;
    private final String password;

    @Setter
    private boolean devMode;

    public SimpleDb(String host, String username, String password, String dbName) {
        this.url = String.format("jdbc:mysql://%s:3306/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Seoul", host, dbName);
        this.username = username;
        this.password = password;
    }

    public void run(String sql) {
        if (devMode) {
            System.out.println("SQL: " + sql);
        }

        // try-with-resources를 사용하여 Connection과 Statement를 자동으로 닫습니다.
        try (Connection conn = DriverManager.getConnection(url, username, password);
             Statement stmt = conn.createStatement()) {

            stmt.execute(sql);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}