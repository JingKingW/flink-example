package com.xunmall.example.flink.streaming.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/22 15:41
 */
public class CommonApiExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 基于老版本planner的流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);

        //  1.2 基于老版本planner批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);

        // 1.3基于Blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        // 1.4基于Blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2 表的创建，连接外部系统，读取数据
        // 读取文件
        String filePath = CommonApiExample.class.getResource("/sensorReading.txt").getPath();
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("name", DataTypes.STRING())
                        .field("value", DataTypes.DOUBLE())
                        .field("timestamp", DataTypes.BIGINT()))
                .createTemporaryTable("inputTable");
        Table inputTable = tableEnv.from("inputTable");
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();
        // 查询转换
        // 3.1 Table API
        // 简单转换
        Table filter = inputTable.select("name,'timestamp'").filter("name === 'sensor_1'");
        // 聚合操作
        Table select = inputTable.groupBy("name").select("name,name.count as count");
        // 3.2 SQL
        Table sqlQuery1 = tableEnv.sqlQuery("select name,'timestamp' from inputTable where name = 'sensor_1'");
        Table sqlQuery2 = tableEnv.sqlQuery("select name from inputTable where name = 'sensor_1'");

        tableEnv.toAppendStream(filter,Row.class).print("filter");
        tableEnv.toRetractStream(select,Row.class).print("select");
        tableEnv.toRetractStream(sqlQuery1,Row.class).print("sqlQuery1");
        tableEnv.toRetractStream(sqlQuery2,Row.class).print("sqlQuery2");
        env.execute();
    }


}
