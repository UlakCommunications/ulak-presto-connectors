package com.facebook.presto.pg;

import org.apache.commons.dbcp.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class DBCPDataSource {

    private static BasicDataSource ds = null;

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    DBCPDataSource(String url, String user, String pwd) {

        ds = new BasicDataSource();
        ds.setUrl(url);
        ds.setUsername(user);
        ds.setPassword(pwd);
        ds.setMinIdle(20);
        ds.setMaxIdle(30);
        ds.setMaxOpenPreparedStatements(128);
    }
}