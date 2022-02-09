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
 * @description 连续温度上升告警
 * @date 2020/12/21 16:14
 */
public class ApplicationCaseExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<String> inputStream = executionEnvironment.socketTextStream("192.168.79.129", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Double(fields[1]), new Long(fields[2]));
        });

        SingleOutputStreamOperator<String> processStream = dataStream.keyBy("name").process(new TempConsIncreWarning(10));

        processStream.print();

        executionEnvironment.execute();
    }

    public static class TempConsIncreWarning extends KeyedProcessFunction<Tuple,SensorReading,String>{

        private Integer interval;

        public TempConsIncreWarning(Integer interval){
            this.interval = interval;
        }

        private ValueState<Double> lastTempState;

        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class,Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts",Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            // 如果温度上升并且没有定期器时候注册10秒的计时器，开始等待
            if (value.getValue() > lastTemp && timerTs == null){
                Long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }else  if( value.getValue() < lastTemp && timerTs !=null){
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }
            // 更新温度状态
            lastTempState.update(value.getValue());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续" + interval + "s上升");
            timerTsState.clear();
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }






}
