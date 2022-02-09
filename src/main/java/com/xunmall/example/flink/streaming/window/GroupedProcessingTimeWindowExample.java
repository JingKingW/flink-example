package com.xunmall.example.flink.streaming.window;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author wangyj03@zenmen.com
 * @description 时间窗口统计商品类别和销售量
 * @date 2020/12/10 14:18
 */
public class GroupedProcessingTimeWindowExample {

    /**
     *  模拟数据源，随机产生三种类别商品及销售量
     */
    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
            Random random = new Random();
            while (isRunning) {
                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);
                String key = "类别" + (char) ('A' + random.nextInt(3));
                int value = random.nextInt(10) + 1;
                System.out.println(String.format("Emits\t(%s,%d)", key, value));
                sourceContext.collect(new Tuple2<>(key, value));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static void main(String[] args) throws Exception {
        // 构建环境并设置该task的并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 接入数据源
        DataStream<Tuple2<String, Integer>> ds = env.addSource(new DataSource());
        // 根据key进行分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = ds.keyBy(0);
        // 进行算子计算
        keyedStream.sum(1).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return "";
            }
        }).fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> fold(HashMap<String, Integer> stringIntegerHashMap, Tuple2<String, Integer> o) throws Exception {
                stringIntegerHashMap.put(o.f0, o.f1);
                return stringIntegerHashMap;
            }
        }).addSink(new SinkFunction<HashMap<String, Integer>>() {
            @Override
            public void invoke(HashMap<String, Integer> value, Context context) throws Exception {
                System.out.println(value);
                System.out.println(value.values().stream().mapToInt(v -> v).sum());
            }
        });

        env.execute();
    }


}
