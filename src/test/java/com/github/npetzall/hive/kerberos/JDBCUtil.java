package com.github.npetzall.hive.kerberos;

import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

public class JDBCUtil {

    public static void createAndValidateTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("create table hiveKerberosTest (key int, value string)");
            try (ResultSet resultSet = connection.getMetaData().getTables(null, "default", null, new String[]{"TABLE"})) {
                resultSet.next();
                String tableName = resultSet.getString("TABLE_NAME");
                assertThat(tableName).isEqualToIgnoringCase("hiveKerberosTest");
            }
            statement.execute("drop table if exists hiveKerberosTest");
            try (ResultSet resultSet = connection.getMetaData().getTables(null, "default", null, new String[]{"TABLE"})) {
                assertThat(resultSet.next()).isFalse();
            }
        }
    }
}
