package com.xunmall.example.flink.streaming.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/22 16:46
 */
public class KafkaPipelineExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect", "192.169.79.130:2181")
                .property("bootstrap.servers", "192.168.79.130:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("name", DataTypes.STRING())
                        .field("value", DataTypes.DOUBLE())
                        .field("timestamp", DataTypes.BIGINT()))
                .createTemporaryTable("kafkaTable");

        Table inputTable = tableEnv.from("kafkaTable");

        Table table = inputTable.groupBy("name").select("name,name.count as count,value.avg as avgTmp");

        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor_out")
                .property("zookeeper.connect", "192.169.79.130:2181")
                .property("bootstrap.servers", "192.168.79.130:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("name", DataTypes.STRING())
                        .field("avgTmp", DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        table.insertInto("outputTable");

        env.execute();
    }


}
