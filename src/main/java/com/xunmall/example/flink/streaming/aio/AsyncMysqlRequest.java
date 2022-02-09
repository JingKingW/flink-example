package com.xunmall.example.flink.streaming.aio;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/1/10 15:55
 */
public class AsyncMysqlRequest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer010<String> kafkaDataSource = new FlinkKafkaConsumer010<String>("dhCalcNodeNumber",new SimpleStringSchema(),consumerConfig());

        DataStreamSource<String> dataStreamSource = environment.addSource(kafkaDataSource);

        DataStream<User> userDataStream = dataStreamSource.map(new MapFunction<String, User>() {
            @Override
            public User map(String value) throws Exception {
                String[] fields = value.split(",");
                return new User(fields[0],fields[1],fields[2]);
            }
        });

        SingleOutputStreamOperator<AsyncUser> streamOperator = AsyncDataStream.unorderedWait(userDataStream, new AsyncFunctionForMysql(), 1000, TimeUnit.MICROSECONDS, 10);

        streamOperator.print("async user");

        environment.execute();

    }

    private static Properties consumerConfig(){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.79.129:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"ft_group");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        return properties;
    }
}
