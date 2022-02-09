package com.xunmall.example.flink.streaming.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author wangyj03@zenmen.com
 * @description 自定义扩展数据源
 * @date 2020/12/9 10:26
 */
public class MyRichParalleSource extends RichParallelSourceFunction<Long> {

    private long count = 1L;

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while(isRunning){
            sourceContext.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open......");
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
