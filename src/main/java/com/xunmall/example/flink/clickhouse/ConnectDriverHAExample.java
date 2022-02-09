package com.xunmall.example.flink.clickhouse;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/18 15:32
 */
public class ConnectDriverHAExample {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String url = "jdbc:clickhouse://server3:8123,server1:8123/default";
        ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        clickHouseProperties.setUser("default");
        BalancedClickhouseDataSource balancedClickhouseDataSource = new BalancedClickhouseDataSource(url,clickHouseProperties);
        // 对每个链接进行验证是否可用
        balancedClickhouseDataSource.actualize();
        Connection con = balancedClickhouseDataSource.getConnection();
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("SELECT 1");
        rs.next();
        System.out.println("res " + rs.getInt(1));
    }


}
