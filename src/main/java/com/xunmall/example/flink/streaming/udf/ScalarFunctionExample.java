package com.xunmall.example.flink.streaming.udf;

import com.xunmall.example.flink.SensorReading;
import com.xunmall.example.flink.streaming.table.FileOutputExample;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/23 13:20
 */
public class ScalarFunctionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        Table table = tableEnvironment.fromDataStream(dataStream, "name,value,timestamp as ts");

        // 注册函数
        tableEnvironment.registerFunction("hashcode", new HashScalarFunction(10));
        // TABLE API
        Table resultTable = table.select("name,value,ts,hashcode(name)");

        // SQL
        tableEnvironment.createTemporaryView("sensor",table);
        Table resultSql = tableEnvironment.sqlQuery("select name,`value`,ts,hashcode(name) from sensor");

        tableEnvironment.toAppendStream(resultTable, Row.class).print("table");
        tableEnvironment.toAppendStream(resultSql, Row.class).print("sql");

        environment.execute();
    }

    public static class HashScalarFunction extends ScalarFunction {

        private int factor = 13;

        public HashScalarFunction(int factor) {
            this.factor = factor;
        }

        public int eval(String param) {
            return param.hashCode() * factor;
        }


    }
}
