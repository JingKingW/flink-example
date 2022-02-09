package com.xunmall.example.flink.batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * @author wangyj03@zenmen.com
 * @description 全局累加器
 * @date 2020/12/10 15:35
 */
public class BatchCounterExample {

    public static void main(String[] args) throws Exception {
        // 运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 数据源
        DataSource<String> data = env.fromElements("a","b","c","d");
        // 转换
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            // 创建累加器
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 注册累加器
                getRuntimeContext().addAccumulator("num-lines",this.numLines);
            }

            @Override
            public String map(String s) throws Exception {
                // 如果并行度为1，则使用普通的累加求和即可;如果设置多个并行度，则普通的累加求和就不准确
                this.numLines.add(1);
                return s;
            }
        }).setParallelism(8);

        result.writeAsText("D:\\xm\\flink-example\\src\\main\\resources\\counter10");

        JobExecutionResult jobResult = env.execute("counter");
        int num = jobResult.getAccumulatorResult("num-lines");
        System.out.println("num: " + num);









    }







}
