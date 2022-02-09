package com.xunmall.example.flink.streaming.table;

import com.xunmall.example.flink.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/22 14:35
 */
public class TableAPIExample2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<String> inputStream = executionEnvironment.readTextFile("D:\\xm\\flink-example\\src\\main\\resources\\sensorReading.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Double(fields[1]), new Long(fields[2]));
        });

        // 创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(executionEnvironment);
        // 基于数据流创建表
        Table fromDataStream = tableEnvironment.fromDataStream(dataStream);
        // 调用table api进行操作
        Table resultTable = fromDataStream.select("name,'value'").where("name = 'sensor_1'");
        // 执行SQL
        tableEnvironment.createTemporaryView("sensor",fromDataStream);
        String sql ="select name,'value' from sensor where name = 'sensor_1'";
        Table resultSql = tableEnvironment.sqlQuery(sql);

        tableEnvironment.toAppendStream(resultTable,Row.class).print("result");
        tableEnvironment.toAppendStream(resultSql,Row.class).print("sql");

        executionEnvironment.execute();
    }

}
