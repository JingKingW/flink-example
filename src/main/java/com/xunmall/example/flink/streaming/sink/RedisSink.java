package com.xunmall.example.flink.streaming.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author wangyj03@zenmen.com
 * @description 可扩站Sink，使用Redis实现
 * @date 2020/12/9 11:15
 */
public class RedisSink<IN> extends RichSinkFunction<IN> {

    private static final Logger logger = LoggerFactory.getLogger(RedisSink.class);

    private String additionalKey;
    private RedisMapper<IN> redisMapper;
    private RedisCommand redisCommand;

    private FlinkJedisConfigBase flinkJedisConfigBase;
    private RedisCommandsContainer redisCommandsContainer;

    public RedisSink(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<IN> redisMapper) {
        Objects.requireNonNull(flinkJedisConfigBase, "Redis connection pool config should not be null");
        Objects.requireNonNull(redisMapper, "Redis Mapper can not be null");
        Objects.requireNonNull(redisMapper.getCommandDescription(), "Redis Mapper data type description can not be null");
        this.flinkJedisConfigBase = flinkJedisConfigBase;
        this.redisMapper = redisMapper;
        RedisCommandDescription redisCommandDescription = redisMapper.getCommandDescription();
        this.redisCommand = redisCommandDescription.getCommand();
        this.additionalKey = redisCommandDescription.getAdditionalKey();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.redisCommandsContainer = RedisCommandsContainerBuilder.build(this.flinkJedisConfigBase);
        this.redisCommandsContainer.open();
    }

    @Override
    public void close() throws Exception {
        if (redisCommandsContainer != null) {
            redisCommandsContainer.close();
        }
    }

    @Override
    public void invoke(IN input) throws Exception {
        String key = redisMapper.getKeyFromData(input);
        String value = redisMapper.getValueFromData(input);

        switch (redisCommand) {
            case RPUSH:
                this.redisCommandsContainer.rpush(key, value);
                break;
            case LPUSH:
                this.redisCommandsContainer.lpush(key, value);
                break;
            case SADD:
                this.redisCommandsContainer.sadd(key, value);
                break;
            case SET:
                this.redisCommandsContainer.set(key, value);
                break;
            case PFADD:
                this.redisCommandsContainer.pfadd(key, value);
                break;
            case PUBLISH:
                this.redisCommandsContainer.publish(key, value);
                break;
            case ZADD:
                this.redisCommandsContainer.zadd(this.additionalKey, key, value);
                break;
            case ZREM:
                this.redisCommandsContainer.zrem(this.additionalKey, key);
                break;
            case HSET:
                this.redisCommandsContainer.hset(this.additionalKey, key, value);
                break;
            default:
                throw new IllegalArgumentException("Cannot process such data type:" + redisCommand);
        }
    }
}
