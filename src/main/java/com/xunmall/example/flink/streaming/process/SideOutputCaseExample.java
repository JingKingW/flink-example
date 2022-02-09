package com.xunmall.example.flink.streaming.process;

import com.xunmall.example.flink.MySensorSource;
import com.xunmall.example.flink.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/21 16:42
 */
public class SideOutputCaseExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStream<SensorReading> dataStream = executionEnvironment.addSource(new MySensorSource());

        // 定义一个OutputTag ,用来表示侧输出流
        OutputTag<SensorReading> lowTmpTag = new OutputTag<SensorReading>("lowTemp") {
        };

        SingleOutputStreamOperator<SensorReading> highTempStream = dataStream.timeWindowAll(Time.seconds(5)).process(new ProcessAllWindowFunction<SensorReading, SensorReading, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<SensorReading> elements, Collector<SensorReading> out) throws Exception {
                elements.forEach(value -> {
                    if (value.getValue() > 40) {
                        out.collect(value);
                    } else {
                        context.output(lowTmpTag, value);
                    }
                });
            }
        });
        highTempStream.print("high-temp");
        highTempStream.getSideOutput(lowTmpTag).print("low-temp");

        executionEnvironment.execute();
    }

}
