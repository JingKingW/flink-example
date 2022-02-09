package com.xunmall.example.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.DecimalFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/21 10:22
 */
public class MySensorSource implements SourceFunction<SensorReading> {
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        Random random = new Random();
        Map<String, Double> initData = new LinkedHashMap<>();
        for (int i = 0; i < 10; i++) {
            initData.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
        }
        while (isRunning) {
            for (String key : initData.keySet()) {
                Double nextTemp = initData.get(key) + random.nextGaussian();
                String value = new DecimalFormat("#0.00").format(nextTemp);
                initData.put(key, nextTemp);
                sourceContext.collect(new SensorReading(key, Double.valueOf(value), System.currentTimeMillis()));
            }
            Thread.sleep(random.nextInt(1000));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
