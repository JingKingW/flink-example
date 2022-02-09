package com.xunmall.example.flink.streaming.state;

import com.xunmall.example.flink.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/21 14:18
 */
public class KeyedStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<String> inputStream = executionEnvironment.socketTextStream("192.168.79.129", 7777);

        DataStream<SensorReading> dataStream  = inputStream.map(line ->{
            String[] fields = line.split(",");
            return  new SensorReading(fields[0],new Double(fields[1]),new Long(fields[2]));
        });

        SingleOutputStreamOperator<Integer> map = dataStream.keyBy("name").map(new MyKeyCountMapper());

        map.print();

        executionEnvironment.execute();
    }

    private static class MyKeyCountMapper extends RichMapFunction<SensorReading,Integer>{

        private ValueState<Integer> keyCountState;

        @Override
        public Integer map(SensorReading value) throws Exception {
            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("mykeyed-count",Integer.class,0));
        }
    }
}
