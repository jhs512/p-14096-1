package com.ll.simpleDb;

import lombok.Setter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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

    // 가변 인자(Object... args)를 추가하여 파라미터 바인딩 지원
    public void run(String sql, Object... args) {
        if (devMode) {
            System.out.println("SQL: " + sql);
        }

        // PreparedStatement를 사용하여 ? 에 값을 채워넣습니다.
        try (Connection conn = DriverManager.getConnection(url, username, password);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 인자로 들어온 값들을 순서대로 바인딩 (index는 1부터 시작)
            for (int i = 0; i < args.length; i++) {
                pstmt.setObject(i + 1, args[i]);
            }

            pstmt.execute();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}