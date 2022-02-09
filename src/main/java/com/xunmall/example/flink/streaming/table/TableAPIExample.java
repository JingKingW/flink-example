package com.xunmall.example.flink.streaming.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/9 16:27
 */
public class TableAPIExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        TableSource csvSource = new CsvTableSource("D:\\xm\\flink-example\\src\\main\\resources\\table_student.csv",new String[]{"name","age"},new TypeInformation[]{Types.STRING,Types.INT});

        tableEnv.registerTableSource("CsvTable",csvSource);

        Table csvTable = tableEnv.scan("CsvTable");
        Table csvResult = csvTable.select("name,age");

        DataStream<Student> csvStream = tableEnv.toAppendStream(csvResult,Student.class);

        csvStream.print().setParallelism(1);

        env.execute("csvStream");

    }

    public static class Student{
        public String name;
        public int age;

        public Student(){}

        public Student(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "name='" + name + '\'' +
                    ", age='" + age + '\'' +
                    '}';
        }
    }

}
