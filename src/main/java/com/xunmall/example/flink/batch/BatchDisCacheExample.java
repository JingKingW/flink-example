package com.xunmall.example.flink.batch;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wangyj03@zenmen.com
 * @description 批处理获取缓存中数据
 * @date 2020/12/10 16:22
 */
public class BatchDisCacheExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.registerCachedFile("D:\\xm\\flink-example\\src\\main\\resources\\abc.txt","abc.txt");

        DataSource<String> data = env.fromElements("a","b","c","d");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            private ArrayList<String> dataList = new ArrayList<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File myFile = getRuntimeContext().getDistributedCache().getFile("abc.txt");
                List<String> lines = FileUtils.readLines(myFile);
                for(String line : lines){
                    this.dataList.add(line);
                    System.out.println("line:" + line);
                }
            }

            @Override
            public String map(String s) throws Exception {
                return s;
            }
        });

        result.print();




    }



}
