package com.alibabacloud.polar_race.engine.kv.partition;

public interface Partition {
    int partition(long value);
}
