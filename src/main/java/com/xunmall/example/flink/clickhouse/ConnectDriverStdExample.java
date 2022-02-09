package com.xunmall.example.flink.clickhouse;

import java.sql.*;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/18 15:08
 */
public class ConnectDriverStdExample {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String url = "jdbc:clickhouse://192.168.79.129:8123/default";
        String user = "default";
        String password ="";
        Connection con = DriverManager.getConnection(url,user,password);
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("SELECT 1");
        rs.next();
        System.out.println("res " + rs.getInt(1));
    }
}
