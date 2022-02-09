package com.xunmall.example.flink.phoenix;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/1/8 20:37
 */
public class ConnectionTest {
    Connection connection = null;

    PreparedStatement ps = null;

    ResultSet rs = null;

    @Before
    public void init() throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection("jdbc:phoenix:192.168.79.129:2181");
    }

    /**
     * 查询已有的表
     *
     * @throws Exception
     */
    @Test
    public void testQuery() throws Exception {
        String sql = "select * from PHONE_LABEL limit 10";
        try {
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                String PHONEKEY = rs.getString("PHONEKEY");
                String IC = rs.getString("IC");
                String PHONE = rs.getString("PHONE");
                System.out.println("PHONEKEY:" + PHONEKEY);
                System.out.println("IC:" + IC);
                System.out.println("PHONE:" + PHONE);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Test
    public void testUpdate() throws Exception {
        String sql = "upsert into PHONE_LABEL(PHONEKEY,PHONE,UPDATETIME) values (?,?,NOW())";
        try {
            ps = connection.prepareStatement(sql);
            ps.setString(1, "1001");
            ps.setString(2, "155xxxx8888");
            ps.executeUpdate();
            connection.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (connection != null) {
                connection.close();
            }
        }

    }

    /**
     * 建表并查询
     *
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        Statement statement = connection.createStatement();
        statement.executeUpdate("create  table test(id integer primary key ,animal varchar )");

        //新增和更新都是一个操作：upsert
        statement.executeUpdate("upsert into test values (1,'dog')");
        statement.executeUpdate("upsert into test values (2,'cat')");
        connection.commit();

        PreparedStatement preparedStatement = connection.prepareStatement("select * from  test");
        rs = preparedStatement.executeQuery();
        while (rs.next()) {
            String id = rs.getString("id");
            String animal = rs.getString("animal");
            String format = String.format("id:%s,animal:%s", id, animal);
            System.out.println(format);
        }
    }

    /**
     * 删除数据
     *
     * @throws Exception
     */
    @Test
    public void delete() throws Exception {
        try {
            ps = connection.prepareStatement("delete from test where id=2");
            ps.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * 删除表
     *
     * @throws Exception
     */
    @Test
    public void dropTable() throws Exception {
        try {
            ps = connection.prepareStatement("drop table test");
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    @After
    public void close() throws Exception {
        if (rs != null) {
            rs.close();
        }
        if (ps != null) {
            ps.close();
        }
        if (connection != null) {
            connection.close();
        }

    }


}
