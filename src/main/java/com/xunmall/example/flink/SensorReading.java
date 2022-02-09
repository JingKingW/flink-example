package com.xunmall.example.flink;

/**
 * @author wangyj03@zenmen.com
 * @description
 * @date 2020/12/18 16:03
 */
public class SensorReading {
    private String name;
    private Double value;
    private Long timestamp;

    public SensorReading() {
    }

    public SensorReading(String name, Double value, Long timestamp) {
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "name='" + name + '\'' +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
