package com.xunmall.example.flink.util;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2021/2/26 15:35
 */
public class ParseTableNameUtils {

    public static String parseTableNameByKey(String key) {
        // 采用Hashcode取模方式进行分表存储
        int hash = Math.abs(key.hashCode()) % 3;
        // 判断数据满足业务类型，再分别组装表名称
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("sensor_temp");
        stringBuilder.append(hash);
        String tableName = stringBuilder.toString();
        return tableName;
    }

    public static void main(String[] args) {
        System.out.println(parseTableNameByKey("sensor_1"));
    }

}
