package com.xunmall.example.flink.streaming.sink;

import com.google.common.collect.Lists;
import com.xunmall.example.flink.MySensorSource;
import com.xunmall.example.flink.SensorReading;
import com.xunmall.example.flink.util.ParseTableNameUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/2/26 15:58
 */
public class StreamingToMysqlExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        SingleOutputStreamOperator<List<SensorReading>> apply = dataStream.timeWindowAll(Time.seconds(2)).apply(new AllWindowFunction<SensorReading, List<SensorReading>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<SensorReading> values, Collector<List<SensorReading>> out) throws Exception {
                List<SensorReading> sensorReadingList = Lists.newArrayList(values);
                if (sensorReadingList.size() > 0) {
                    out.collect(sensorReadingList);
                }
            }
        });

        apply.addSink(new MyJdbcSink());

        env.execute();
    }

    // 实现自定义的SinkFunction
    public static class MyJdbcSink extends RichSinkFunction<List<SensorReading>> {

        Connection connection = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            String url = "jdbc:mysql://192.168.79.130:3306/db_high_node";
            connection = DriverManager.getConnection(url, "wangyanjing", "Abc123!@#");
            connection.setAutoCommit(false);
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (connection != null) {
                connection.close();
            }
        }

        @Override
        public void invoke(List<SensorReading> value, Context context) throws Exception {
            Map<String, List<SensorReading>> resultList = value.stream().collect(Collectors.groupingBy(item->{
                return ParseTableNameUtils.parseTableNameByKey(item.getName());
            }));
            if (resultList.size() > 0){
                for (Map.Entry<String,List<SensorReading>> entry : resultList.entrySet()){
                    System.out.println(">>>>>>>>>>>>>" + entry.getKey()+ entry.getValue());
                    batchUpdateMysql(entry.getValue(),entry.getKey());
                }
            }
        }


        private void batchInsertMysql(List<SensorReading> list, String tableName) throws Exception {
            String insertSql = "insert into " + tableName + " (id,temp) values (?,? )";
            PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
            for (SensorReading sensorReading : list) {
                preparedStatement.setString(1, sensorReading.getName());
                preparedStatement.setDouble(2, sensorReading.getValue());
                preparedStatement.addBatch();
            }
            int[] count = preparedStatement.executeBatch();
            System.out.println("成功了插入了" + count.length + "行数据");
            preparedStatement.close();
        }

        private void batchUpdateMysql(List<SensorReading> list, String tableName) throws Exception {
            String updateSql = "update " + tableName +" set temp = ? where id = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(updateSql);
            for (SensorReading sensorReading : list) {
                preparedStatement.setDouble(1, sensorReading.getValue());
                preparedStatement.setString(2, sensorReading.getName());
                preparedStatement.addBatch();
            }
            int[] count = preparedStatement.executeBatch();
            System.out.println("成功了更新了" + count.length + "行数据");
            preparedStatement.close();
        }
    }


}
