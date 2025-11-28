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
        return DriverManager.getConnection(url, username, password);
    }

    // 가변 인자(Object... args)를 추가하여 파라미터 바인딩 지원
    public void run(String sql, Object... args) {
        genSql()
                .append(sql, args)
                .execute();
    }
}