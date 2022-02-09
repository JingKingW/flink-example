package com.xunmall.example.flink.streaming.window;

import com.xunmall.example.flink.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/18 15:58
 */
public class EventTimeWindowExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        executionEnvironment.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<String> inputStream = executionEnvironment.socketTextStream("192.168.79.129", 7777);

        // 转换成SensorReading类型，分配时间戳和watermark
        DataStream<SensorReading> dataStream  = inputStream.map(line ->{
            String[] fields = line.split(",");
            return  new SensorReading(fields[0],new Double(fields[1]),new Long(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(SensorReading sensorReading) {
                return sensorReading.getTimestamp();
            }
        });

        // 基于事件时间的开窗聚合,统计15秒时间内的温度最小值
        SingleOutputStreamOperator<SensorReading> sensorReadingSingleOutputStreamOperator = dataStream.keyBy("name").timeWindow(Time.seconds(15)).minBy("timestamp");
        sensorReadingSingleOutputStreamOperator.print("minTemp");

        executionEnvironment.execute();
    }

}
