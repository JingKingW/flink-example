package com.xunmall.example.flink.streaming.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/3/4 9:42
 */
public class FileOutputExample {

    public static void main(String[] args) throws Exception {
        // 执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // 表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 创建表
        String filePath = FileOutputExample.class.getResource("/sensorReading.txt").getPath();
        tableEnvironment.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema().field("name", DataTypes.STRING()).field("value", DataTypes.DOUBLE()).field("timestamp",DataTypes.BIGINT()))
                .createTemporaryTable("inputTable");
        // 获取表
        Table inputTable = tableEnvironment.from("inputTable");
        // 执行简单语句
        Table filter = inputTable.select("name,timestamp").filter("name === 'sensor_1'");
        // 输出打印
        tableEnvironment.toAppendStream(filter,Row.class).print("filter");

        // 输出到文件
        String outFile = "D:\\xm\\flink-example\\src\\main\\resources\\output.txt";
        tableEnvironment.connect(new FileSystem().path(outFile))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("name", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT()))
                .createTemporaryTable("outputTable");

        filter.insertInto("outputTable");


        environment.execute();

    }




}
