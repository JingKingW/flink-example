package com.xunmall.example.flink.streaming.window;

import com.xunmall.example.flink.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/18 15:50
 */
public class TimeWindowExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<String> inputStream = executionEnvironment.socketTextStream("192.168.79.129", 7777);

        DataStream<SensorReading> dataStream  = inputStream.map(line ->{
            String[] fields = line.split(",");
            return  new SensorReading(fields[0],new Double(fields[1]),new Long(fields[2]));
        });

        // 增量窗口
        DataStream<Integer> aggregate = dataStream.keyBy("name")
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
            @Override
            public Integer createAccumulator() {
                return 0;
            }

            @Override
            public Integer add(SensorReading sensorReading, Integer integer) {
                return integer + 1;
            }

            @Override
            public Integer getResult(Integer integer) {
                return integer;
            }

            @Override
            public Integer merge(Integer integer, Integer acc1) {
                return integer + acc1;
            }
        });

        // 全窗口函数
        SingleOutputStreamOperator<Tuple3<String,Long,Integer>> streamOperator = dataStream
                .keyBy("name")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String,Long,Integer>> collector) throws Exception {
                String name = tuple.getField(0);
                Long windowEnd = timeWindow.getEnd();
                Integer count = IteratorUtils.toList(iterable.iterator()).size();
                collector.collect(new Tuple3<>(name,windowEnd,count));
            }
        });

        //aggregate.print();
        streamOperator.print();

        executionEnvironment.execute();
    }


}
