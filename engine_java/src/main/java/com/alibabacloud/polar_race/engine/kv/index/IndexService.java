package com.alibabacloud.polar_race.engine.kv.index;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.kv.partition.RangeIterator;

import java.util.concurrent.CountDownLatch;

public interface IndexService {

    /***
     * @return  offset of the key
     *
     **/
    int getOffset(long key) throws EngineException;

    /**
     *
     *
     **/
    void range(long lower, long upper, RangeIterator iterator);

    /**
     * partition index
     *
     **/
    void startPartition(CountDownLatch startLatch) throws Exception;

    /**
     * load index into memory
     *
     **/
    void loadIndex(CountDownLatch loadLatch) throws Exception;
}
