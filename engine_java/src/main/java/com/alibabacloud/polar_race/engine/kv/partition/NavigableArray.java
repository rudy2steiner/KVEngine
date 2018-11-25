package com.alibabacloud.polar_race.engine.kv.partition;

/***
 * [a,b)
 *
 *
 **/
public interface NavigableArray {
    /**
     * @return  Returns the least key greater than or equal to the given key,
     **/
    int ceiling(long key);

    /**
     * Returns the greatest key less than or equal,
     * */
    int floor(long key);

    /**
     *
     * 子区间, start inclusive,end exclusive
     **/
    void iterate(int start,int end,RangeIterator iterator);


}
