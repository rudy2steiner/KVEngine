package com.alibabacloud.polar_race.engine.kv.index;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

import java.util.concurrent.CountDownLatch;

/**
 *
 * for range
 *
 **/
public class KVRangeIndexService implements IndexService {


    @Override
    public int getOffset(long key) throws EngineException {
        return 0;
    }

    @Override
    public void range(long lower, long upper, AbstractVisitor iterator) {

    }

}
