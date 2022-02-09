package com.xunmall.example.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author wangyj03@zenmen.com
 * @description 单词计数之滑动窗口计算
 * @date 2020/12/7 10:04
 */
public class SocketWindowWordCountExample {

    public static void main(String[] args) throws Exception {
        int port;
        try{
            ParameterTool parameterTool = ParameterTool.fromArgs(args);

            port = parameterTool.getInt("port");

        }catch (Exception ex){
            System.out.println("NO port set, use default port 9000 - java");
            port = 9000;
        }

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname = "192.168.79.129";
        String delimter = "\n";

        DataStreamSource<String> text = environment.socketTextStream(hostname,port,delimter);

        DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> collector) throws Exception {
                String[] splits = value.split(" \\s");
                for (String word : splits){
                    collector.collect(new WordWithCount(word,1L));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(2),Time.seconds(1)).sum("count");

        windowCounts.print().setParallelism(1);

        environment.execute("Socket window count");
    }

    public static class WordWithCount{
        public String word;
        public long count;
        public WordWithCount(){}
        public WordWithCount(String word,long count){
            this.word = word;
            this.count = count;
        }
        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
