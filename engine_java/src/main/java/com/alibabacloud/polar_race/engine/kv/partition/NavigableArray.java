package com.alibabacloud.polar_race.engine.kv.partition;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;

/***
 * [a,b)
 *
 *
 **/
public interface NavigableArray {
    /**
     * @return  the least key greater than or equal to the given key,
     * 大于等于key 的最小位置
     **/
    int ceiling(long key);

    /**
     * @return the greatest key less than or equal,
     * 小于等于key 的最大位置
     * */
    int floor(long key);

    /**
     *
     * 子区间, start inclusive,end exclusive
     **/
    void iterate(long lower, long upper, AbstractVisitor iterator) throws EngineException;
    /**
     *
     * 子区间, start inclusive,end exclusive
     **/
    void iterate(long lower, AbstractVisitor iterator) throws EngineException;

}
