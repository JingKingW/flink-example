package com.xunmall.example.flink.streaming.aio;

import java.sql.*;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/1/11 9:49
 */
public class MysqlClient {

    private static String jdbcUrl = "jdbc:mysql://192.168.79.130:3306?useSSL=false&allowPublicKeyRetrieval=true";
    private static String username = "wangyanjing";
    private static String password = "Abc123!@#";
    private static String driverName = "com.mysql.jdbc.Driver";
    private static Connection connection;
    private static PreparedStatement preparedStatement;

    static {
        try {
            Class.forName(driverName);
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            preparedStatement = connection.prepareStatement("select phone from async.async_test where id = ?");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public AsyncUser queryUser(User user) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String phone = "0000";
        try {
            preparedStatement.setString(1, user.getId());
            ResultSet resultSet = preparedStatement.executeQuery();
            if (!resultSet.isClosed() && resultSet.next()) {
                phone = resultSet.getString(1);
            }
            System.out.println("execute query : " + user.getId() + "-2-" + "phone : " + phone + "-" + System.currentTimeMillis());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new AsyncUser(user.getId(), user.getUsername(), user.getPassword(), phone);
    }

    public static void main(String[] args) {
        MysqlClient mysqlClient = new MysqlClient();
        User user = new User("1", "wangyanjing", "abc123");
        long start = System.currentTimeMillis();
        AsyncUser asyncUser = mysqlClient.queryUser(user);
        System.out.println("end : " + (System.currentTimeMillis() - start));
        System.out.println(asyncUser.toString());
    }
}
