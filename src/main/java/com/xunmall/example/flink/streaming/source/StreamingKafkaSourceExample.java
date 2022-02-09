package com.xunmall.example.flink.streaming.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author wangyj03@zenmen.com
 * @description 从Kafka读取源数据新
 * @date 2020/12/11 15:51
 */
public class StreamingKafkaSourceExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "t1";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.79.129:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"con1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest ");

        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<String>(topic,new SimpleStringSchema(),properties);

        DataStreamSource<String> text = env.addSource(myConsumer);

        text.print().setParallelism(1);

        env.execute("StreamingKafkaSource");

    }

}
