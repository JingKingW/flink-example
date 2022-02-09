package com.xunmall.example.flink.streaming.transform;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @author wangyj03@zenmen.com
 * @description 自定义分组方法
 * @date 2020/12/9 10:53
 */
public class MyPartition implements Partitioner<Long> {
    @Override
    public int partition(Long aLong, int i) {
        System.out.println("分区总数" + i);

        if (aLong % 2 == 0){
            return 0;
        }else {
            return 1;
        }
    }
}
