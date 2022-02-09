package com.xunmall.example.flink.streaming.table;

import com.xunmall.example.flink.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/3/17 9:42
 */
public class TimeAndWindowExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        String filePath = FileOutputExample.class.getResource("/sensorReading.txt").getPath();
        DataStreamSource<String> inputStream = environment.readTextFile(filePath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Double(fields[1]), new Long(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {

            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp();
            }
        });

        // tab dubbo ProcessTime or EventTime
        //Table table = tableEnvironment.fromDataStream(dataStream, "name,value,timestamp,pt.proctime");
        Table table = tableEnvironment.fromDataStream(dataStream, "name,value,timestamp,rt.rowtime");

        table.printSchema();

        tableEnvironment.toAppendStream(table, Row.class).print();

        environment.execute();
    }
}
