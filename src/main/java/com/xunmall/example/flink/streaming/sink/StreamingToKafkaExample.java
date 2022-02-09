package com.xunmall.example.flink.streaming.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author wangyj03@zenmen.com
 * @description 将数据转到Kafka中
 * @date 2020/12/11 16:46
 */
public class StreamingToKafkaExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("192.168.79.129",9000,"\n");

        String borkerList = "192.168.79.129:9092";
        String topic = "t1";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,borkerList);

        FlinkKafkaProducer011 producer011 = new FlinkKafkaProducer011(topic,new SimpleStringSchema(),properties);

        text.addSink(producer011);

        env.execute("StreamingKafkaSink");
    }

}
