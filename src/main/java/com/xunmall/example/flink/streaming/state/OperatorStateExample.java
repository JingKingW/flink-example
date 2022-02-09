package com.xunmall.example.flink.streaming.state;

import com.xunmall.example.flink.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/21 12:45
 */
public class OperatorStateExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        DataStreamSource<String> inputStream = executionEnvironment.socketTextStream("192.168.79.129", 7777);

        DataStream<SensorReading> dataStream  = inputStream.map(line ->{
            String[] fields = line.split(",");
            return  new SensorReading(fields[0],new Double(fields[1]),new Long(fields[2]));
        });
        SingleOutputStreamOperator<Integer> map = dataStream.map(new MyCountMapper());

        map.print();

        executionEnvironment.execute();
    }

    public static class MyCountMapper implements MapFunction<SensorReading,Integer>,ListCheckpointed<Integer>{

        // 定义一个本地变量，作为算子的状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            count++;
            return count;
        }

        @Override
        public List<Integer> snapshotState(long l, long l1) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> list) throws Exception {
                for (Integer stat : list){
                    count = count + stat;
                }
        }
    }
}
