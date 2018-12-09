package com.alibabacloud.polar_race.engine.kv.index;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;


/**
 *
 **/
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
    void range(long lower, long upper, AbstractVisitor iterator);

}
