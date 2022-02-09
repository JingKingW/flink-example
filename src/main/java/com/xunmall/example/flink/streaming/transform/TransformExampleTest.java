package com.xunmall.example.flink.streaming.transform;

import com.xunmall.example.flink.MySensorSource;
import com.xunmall.example.flink.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/16 14:24
 */
public class TransformExampleTest {
    private StreamExecutionEnvironment env;

    private DataStream<SensorReading> dataStream;

    private DataStreamSource<String> inputDataStream;

    @Before
    public void initData() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        dataStream = env.addSource(new MySensorSource());
        String filePath = TransformExampleTest.class.getResource("/sensorReading.txt").getPath();
        inputDataStream = env.readTextFile(filePath);
    }

    @Test
    public void testSimple() throws Exception {
        DataStream<String> mapResult = dataStream.map(new MapFunction<SensorReading, String>() {
            @Override
            public String map(SensorReading sensor) throws Exception {
                return sensor.getName() + "," + sensor.getValue() + "," + sensor.getTimestamp() ;
            }
        });
        DataStream<SensorReading> filterResult = dataStream.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading sensor) throws Exception {
                return sensor.getName().startsWith("sensor_10");
            }
        });
        mapResult.writeAsText("D:\\xm\\flink-example\\src\\main\\resources\\sensorReading.txt");
        filterResult.print();
        env.execute();
    }

    @Test
    public void testKeyByAndMax() throws Exception {
        DataStream<SensorReading> streamOperator = inputDataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0],Double.valueOf(fields[1]),Long.valueOf(fields[2]));
            }
        });
        //  根据属性值进行分组
        KeyedStream<SensorReading, Tuple> sensorTupleKeyedStream = streamOperator.keyBy("name");
        // 求最大温度值
        SingleOutputStreamOperator<SensorReading> outputStreamOperator = sensorTupleKeyedStream.max("timestamp");
        outputStreamOperator.print();
        env.execute();
    }

    @Test
    public void testReduce() throws Exception {
        env.setParallelism(1);
        //  根据属性值进行分组
        KeyedStream<SensorReading, Tuple> keyStream = dataStream.keyBy("name");

        SingleOutputStreamOperator<SensorReading> outputStreamOperator = keyStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getName(), Math.max(value1.getValue(), value2.getValue()), value2.getTimestamp());
            }
        });
        DataStream<SensorReading> map = outputStreamOperator.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading sensor) throws Exception {
                return sensor.getName().equals("sensor_1");
            }
        });
        map.print();
        env.execute();
    }

    /**
     * 分流 按照温度之分解为两条流
     * 合并两条流,流的类型可以不一样，但是只能连接两条流
     *
     * @throws Exception
     */
    @Test
    public void testSplit() throws Exception {
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensor) {
                return (sensor.getValue() > 60 ? Collections.singletonList("high") : Collections.singletonList("low"));
            }
        });

        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");
        highStream.print("high");
        lowStream.print("low");

        DataStream<Tuple2<String, Double>> streamOperator = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getName(), value.getValue());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = streamOperator.connect(lowStream);

        SingleOutputStreamOperator<Object> outputStreamOperator = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getName(), "normal");
            }
        });

        outputStreamOperator.print("connect stream");
        env.execute();
    }

    /**
     * 多条流合并，要求流必须是相同的类型
     * @throws Exception
     */
    @Test
    public void testUnion() throws Exception {
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensor) {
                return (sensor.getValue() > 60 ? Collections.singletonList("high") : Collections.singletonList("low"));
            }
        });

        DataStream<SensorReading> highStream = splitStream.select("high");
        DataStream<SensorReading> lowStream = splitStream.select("low");

        DataStream<SensorReading> union = highStream.union(lowStream);

        union.print("union stream");
        env.execute();
    }



}
