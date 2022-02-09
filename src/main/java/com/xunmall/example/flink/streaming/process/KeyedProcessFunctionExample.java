package com.xunmall.example.flink.streaming.process;

import com.xunmall.example.flink.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/21 15:50
 */
public class KeyedProcessFunctionExample {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<String> inputStream = executionEnvironment.socketTextStream("192.168.79.129", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Double(fields[1]), new Long(fields[2]));
        });

        // 测试keyedprocessfunction,先分组然后自定义处理
        SingleOutputStreamOperator<Integer> process = dataStream.keyBy("name").process(new MyKeyedProcess());

        process.print();

        executionEnvironment.execute();


    }

    public static class MyKeyedProcess extends KeyedProcessFunction<Tuple, SensorReading, Integer> {

        ValueState<Long> tsTimerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer",Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Integer> out) throws Exception {
            out.collect(value.getName().length());
            ctx.timestamp();
            ctx.getCurrentKey();
            // ctx.output();
            ctx.timerService().currentWatermark();
            ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L);
            tsTimerState.update(ctx.timerService().currentProcessingTime() + 1000L);
            //ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            System.out.println(timestamp + " 定时器触发");
            ctx.getCurrentKey();

        }
    }

}
