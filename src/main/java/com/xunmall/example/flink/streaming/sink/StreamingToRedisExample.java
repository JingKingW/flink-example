package com.xunmall.example.flink.streaming.sink;

import com.xunmall.example.flink.MySensorSource;
import com.xunmall.example.flink.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author wangyj03@zenmen.com
 * @description 将数据存储到Redis中
 * @date 2020/12/9 14:15
 */
public class StreamingToRedisExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        DataStream<Tuple2<String,String>> wordsData = dataStream.map(new MapFunction<SensorReading, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>("1_words",sensorReading.toString());
            }
        });

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("10.100.31.12").setPort(6404).setPassword("Ed*C@j5yfW").setDatabase(1).build();

        RedisSink<Tuple2<String,String>> redisSink = new RedisSink<>(config,new MyRedisMapper());

        wordsData.addSink(redisSink);

        env.execute("StreamingToRedisExample");
    }


    public static class MyRedisMapper implements RedisMapper<Tuple2<String,String>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> stringStringTuple2) {
            return stringStringTuple2.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> stringStringTuple2) {
            return stringStringTuple2.f1;
        }
    }

}
