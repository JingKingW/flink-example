package com.xunmall.example.flink.streaming.sink;

import com.xunmall.example.flink.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/2/25 11:23
 */
public class StreamingToJDBCExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String filePath = StreamingToJDBCExample.class.getResource("/sensorReading.txt").getPath();
        DataStream<String> dataStream = env.readTextFile(filePath);
        DataStream<SensorReading> streamOperator = dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Double.valueOf(fields[1]), Long.valueOf(fields[2]));
            }
        });

        streamOperator.addSink(new MyJdbcSink());

        env.execute();
    }

    // 实现自定义的SinkFunction
    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {

        Connection connection = null;

        PreparedStatement insertStatement = null;

        PreparedStatement updateStatement = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            String url = "jdbc:mysql://192.168.79.130:3306/db_high_node";
            connection = DriverManager.getConnection(url,"wangyanjing","Abc123!@#");
            insertStatement = connection.prepareStatement("insert into sensor_temp (id,temp) values (?,? )");
            updateStatement = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (connection !=null){
                connection.close();
            }
            if (insertStatement!=null){
                insertStatement.close();
            }
            if (updateStatement != null){
                updateStatement.close();
            }
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updateStatement.setDouble(1,value.getValue());
            updateStatement.setString(2,value.getName());
            updateStatement.executeUpdate();
            if (updateStatement.getUpdateCount() == 0){
                insertStatement.setString(1,value.getName());
                insertStatement.setDouble(2,value.getValue());
                insertStatement.executeUpdate();
            }
        }
    }

}
