package com.xunmall.example.flink.streaming.transform;

import com.xunmall.example.flink.streaming.source.MyNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangyj03@zenmen.com
 * @description 实现自定义分区读取数据
 * @date 2020/12/9 10:56
 */
public class StreamingWithMyPartitionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource());

        // 转换数据将Long转换成Tuple
        DataStream<Tuple1<Long>> tupleData = text.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long aLong) throws Exception {
                return new Tuple1<>(aLong);
            }
        });

        // 分区之后的数据
        DataStream<Tuple1<Long>> partitionData = tupleData.partitionCustom(new MyPartition(),0);

        DataStream<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程Id:" + Thread.currentThread().getId() + " ,value: " + value);
                return value.getField(0);
            }
        });

        result.print().setParallelism(1);

        env.execute("StreamingWithMyPartitionExample");

    }
}
