package com.xunmall.example.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author wangyj03@zenmen.com
 * @description 单词计数之离线计算
 * @date 2020/12/7 15:06
 */
public class BatchWordCountExample {

    public static void main(String[] args) throws Exception {
        String inputPath = "d:\\data\\abc.txt";
        String outputPath ="d:\\data\\result";

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> text = executionEnvironment.readTextFile(inputPath);

        DataSet<Tuple2<String,Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);

        counts.writeAsCsv(outputPath,"\n"," ").setParallelism(1);

        executionEnvironment.execute("batch word count");

    }


    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens){
                collector.collect(new Tuple2<String,Integer>(token,1));
            }
        }
    }
}
