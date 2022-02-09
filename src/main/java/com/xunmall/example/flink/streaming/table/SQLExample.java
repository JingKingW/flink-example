package com.xunmall.example.flink.streaming.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;


/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/9 18:10
 */
public class SQLExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment bEnv = BatchTableEnvironment.create(env);

        DataSource<String> dataSource = env.readTextFile("D:\\xm\\flink-example\\src\\main\\resources\\student.txt");

        DataSet<Student> inputData = dataSource.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String s) throws Exception {
                String[] splits = s.split(",");
                return new Student(splits[0], Integer.parseInt(splits[1]));
            }
        });

        // 将DataSet转换为Table
        Table table = bEnv.fromDataSet(inputData);
        // 注册student表
        bEnv.registerTable("student", table);
        // 执行slq查询
        Table sqlQuery = bEnv.sqlQuery("select count(1),avg(age) from student");
        // 创建CsvTableSink
        CsvTableSink csvTableSink = new CsvTableSink("D:\\xm\\flink-example\\src\\main\\resources\\result.csv", ",", 1, FileSystem.WriteMode.OVERWRITE);
        // 注册TableSink
        bEnv.registerTableSink("csvOutputTable", new String[]{"count", "avg_age"}, new TypeInformation[]{Types.LONG, Types.INT}, csvTableSink);
        // 把结果数据添加到csvTableSink中
        sqlQuery.insertInto("csvOutputTable");

        env.execute("SQL-Batch");

    }

    public static class Student {
        public String name;
        public int age;

        public Student() {
        }

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
