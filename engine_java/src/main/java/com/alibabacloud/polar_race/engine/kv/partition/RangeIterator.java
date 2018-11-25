package com.alibabacloud.polar_race.engine.kv.partition;

public interface RangeIterator {
    void visit(long key,int offset);
}
