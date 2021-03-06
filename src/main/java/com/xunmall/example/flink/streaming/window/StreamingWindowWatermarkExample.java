package com.xunmall.example.flink.streaming.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author wangyj03@zenmen.com
 * @description 实现水位线相应测试
 * @date 2020/12/11 14:48
 */
public class StreamingWindowWatermarkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置使用Event-time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<String> text = env.socketTextStream("192.168.79.129", 9000, "\n");

        DataStream<Tuple2<String, Long>> inputMap = text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] arr = s.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });


        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            Long currentMaxTimestamp = 0L;
            // 最大允许乱序的时间10s
            final Long maxOutOfOrderness = 10000L;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2, long l) {
                long timestamp = stringLongTuple2.f1;
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                System.out.println("key:" + stringLongTuple2.f0 + ", eventtime:[" + stringLongTuple2.f1 + " | " + sdf.format(stringLongTuple2.f1) + " ] ,cruuetnMaxTimestamp:[" + sdf.format(currentMaxTimestamp) + " ],watermark:[" + getCurrentWatermark().getTimestamp() + " | " + sdf.format(getCurrentWatermark().getTimestamp()) + "]");
                return timestamp;
            }
        });

        DataStream<String> window = waterMarkStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    /**
                     *    window内的数据进行排序
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                        String key = tuple.toString();
                        List<Long> arrayList = new ArrayList<>();
                        Iterator<Tuple2<String, Long>> it = iterable.iterator();
                        while (it.hasNext()) {
                            Tuple2<String, Long> next = it.next();
                            arrayList.add(next.f1);
                        }
                        Collections.sort(arrayList);
                        String pattern;
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = key + "," + arrayList.size() + "," + sdf.format(arrayList.get(0)) + "," + sdf.format(arrayList.get(arrayList.size() - 1)) + ","
                                + sdf.format(timeWindow.getStart()) + "," + sdf.format(timeWindow.getEnd());
                        collector.collect(result);
                    }
                });
         window.print();
         env.execute("eventtime-watermark");
    }


}
