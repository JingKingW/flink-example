package com.xunmall.example.flink.streaming.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author wangyj03@zenmen.com
 * @description 自定义无并行度数据源
 * @date 2020/12/9 10:20
 */
public class MyNoParalleSource implements SourceFunction<Long> {

    private long count = 1L;

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning){
            sourceContext.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
