package com.xunmall.example.flink.streaming.state;

import com.xunmall.example.flink.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/21 15:31
 */
public class FaultToleranceExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        // 设置状态后端
        executionEnvironment.setStateBackend(new MemoryStateBackend());
        executionEnvironment.setStateBackend(new FsStateBackend(""));
        executionEnvironment.setStateBackend(new RocksDBStateBackend(""));

        // 1、检查点配置
        executionEnvironment.enableCheckpointing(300);
        executionEnvironment.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE);

        // 2、高级选项
        executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        executionEnvironment.getCheckpointConfig().setCheckpointTimeout(60000L);
        executionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        executionEnvironment.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        executionEnvironment.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);

        // 3、重启策略配置
        // 固定延迟重启
        executionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L));
        // 失败率重启
        executionEnvironment.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10),Time.minutes(1)));

        DataStreamSource<String> inputStream = executionEnvironment.socketTextStream("192.168.79.129", 7777);

        DataStream<SensorReading> dataStream  = inputStream.map(line ->{
            String[] fields = line.split(",");
            return  new SensorReading(fields[0],new Double(fields[1]),new Long(fields[2]));
        });

        dataStream.print();

        executionEnvironment.execute();
    }
}
