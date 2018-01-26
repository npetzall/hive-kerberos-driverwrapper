package com.github.npetzall.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultSetDumper {
    public static void dumpAll(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        int row = 1;
        while (rs.next()) {
            System.out.println("### ROW: " + row);
            for (int i = 1; i <= columnsNumber; i++) {
                System.out.println(rsmd.getColumnName(i) + ": " + rs.getString(i));
            }
            row++;
        }
    }
}
