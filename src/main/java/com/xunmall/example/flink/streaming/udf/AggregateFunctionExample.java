package com.xunmall.example.flink.streaming.udf;

import com.xunmall.example.flink.SensorReading;
import com.xunmall.example.flink.streaming.table.FileOutputExample;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/23 13:21
 */
public class AggregateFunctionExample {

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

        AvgTemp avgTemp = new AvgTemp();

        // 注册函数
        tableEnvironment.registerFunction("avgTemp", avgTemp);
        // TABLE API
        Table resultTable = table.groupBy("name")
                .aggregate("avgTemp(value) as avgValue")
                .select("name,avgValue");

        // SQL
        tableEnvironment.createTemporaryView("sensor", table);
        Table resultSql = tableEnvironment.sqlQuery("select name,avgTemp(`value`) from sensor group by name");

        tableEnvironment.toRetractStream(resultTable, Row.class).print("table");
        tableEnvironment.toRetractStream(resultSql, Row.class).print("sql");

        environment.execute();
    }

    public static class AvgTemp extends AggregateFunction<Double,Tuple2<Double,Integer>> {


        @Override
        public Double getValue(Tuple2<Double, Integer> doubleIntegerTuple2) {
            return doubleIntegerTuple2.f0 / doubleIntegerTuple2.f1;
        }

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        public void accumulate(Tuple2<Double,Integer> accumulate,Double temp){
            accumulate.f0 += temp;
            accumulate.f1 += 1;

        }
    }

}
