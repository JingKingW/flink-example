package com.xunmall.example.flink.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author wangyj03@zenmen.com
 * @description Broadcast广播变量
 * @date 2020/12/10 15:13
 */
public class BatchBroadcastExample {

    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 准备需要广播的数据
        ArrayList<Tuple2<String,Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("zs",18));
        broadData.add(new Tuple2<>("ls",20));
        broadData.add(new Tuple2<>("ww",17));
        DataSet<Tuple2<String,Integer>> tupleData = env.fromCollection(broadData);
        // 处理需要广播数据，将数据转换成Map类型
        DataSet<HashMap<String,Integer>> toBroadcast = tupleData.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                HashMap<String,Integer> res = new HashMap<>();
                res.put(stringIntegerTuple2.f0,stringIntegerTuple2.f1);
                return res;
            }
        });
        // 数据源
        DataSource<String> data = env.fromElements("zs","ls");
        // 注意，这里使用RichMapFunction获取广播变量
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            List<HashMap<String,Integer>> broadCastMap = new ArrayList<HashMap<String,Integer>>();

            HashMap<String,Integer> allMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (HashMap map : broadCastMap){
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String s) throws Exception {
                Integer age = allMap.get(s);
                return s + "," + age;
            }
        }).withBroadcastSet(toBroadcast,"broadCastMapName");

        result.print();






    }







}
