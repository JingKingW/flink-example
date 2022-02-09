package com.xunmall.example.flink.streaming.sink;

import com.xunmall.example.flink.SensorReading;
import com.xunmall.example.flink.streaming.transform.TransformExampleTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/17 13:25
 */
public class StreamingToEsExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String filePath = TransformExampleTest.class.getResource("/sensorReading.txt").getPath();
        DataStream<String> dataStream = env.readTextFile(filePath);
        DataStream<SensorReading> streamOperator = dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0],Double.valueOf(fields[1]),Long.valueOf(fields[2]));
            }
        });


        // 配置es相关信息
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("10.100.31.32",9201));

        streamOperator.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts,new MyEsSinkFunction()).build());

        env.execute();

    }

    // 实现自定义的ES写入操作
    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading>{

        @Override
        public void process(SensorReading value, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            // 定时写入数据source
            HashMap<String,Object> dataSource = new HashMap<>();
            dataSource.put("name",value.getName());
            dataSource.put("value",value.getValue());
            dataSource.put("timestamp",value.getTimestamp());

            // 创建请求,作为写入的命令
            IndexRequest indexRequest =  Requests.indexRequest().index("zx-data-dh").type("sensorRead").source(dataSource);
            // 发送执行
            requestIndexer.add(indexRequest);
        }
    }

}
