package com.alibabacloud.polar_race.engine.kv.partition;

public interface Partition {
    /**
     * @return  key所在的partition
     **/
    int partition(long key);



}
