package com.xunmall.example.flink.streaming.state;

import com.xunmall.example.flink.MySensorSource;
import com.xunmall.example.flink.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/21 14:44
 */
public class KeyedStateApplicationCaseExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<SensorReading> inputStream = executionEnvironment.addSource(new MySensorSource());

        // 定义一个flatmap操作，检测温度跳变，输出报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> flatMap = inputStream.keyBy("name").flatMap(new TempChangeWarning(2.0));

        flatMap.print();

        executionEnvironment.execute();


    }

    public static class TempChangeWarning extends RichFlatMapFunction<SensorReading,Tuple3<String,Double,Double>>{

        // 私有属性，温度跳变阈值
        private Double threshold;

        public TempChangeWarning(Double threshold){
            this.threshold = threshold;
        }

        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp",Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            // 获取状态
            Double lastTemp = lastTempState.value();

            // 判断状态是不是null
            if (lastTemp != null){
                Double diff = Math.abs(value.getValue() - lastTemp);
                if (diff >= threshold){
                    out.collect(new Tuple3<>(value.getName(),lastTemp,value.getValue()));
                }
            }

            // 更新状态
            lastTempState.update(value.getValue());
        }

        @Override
        public void close() throws Exception {
            super.close();
            lastTempState.clear();
        }
    }

}
