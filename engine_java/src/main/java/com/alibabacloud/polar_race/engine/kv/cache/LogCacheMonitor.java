package com.alibabacloud.polar_race.engine.kv.cache;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.kv.buffer.BufferHolder;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;

import java.util.BitSet;
import java.util.List;

public class LogCacheMonitor implements CacheListener<BufferHolder> {

    private BitSet bitSet;
    private short[] missStat;
    private LogFileService logFileService;

    public LogCacheMonitor(LogFileService logFileService){
        this.logFileService=logFileService;
        this.bitSet=new BitSet(logFileService.allLogFiles().size());
        this.missStat=new short[logFileService.allLogFiles().size()];
    }
    @Override
    public void onMissCache(long id) {
        missStat[(int)(id/StoreConfig.LOG_ELEMNT_LEAST_SIZE)]+=1;
    }

    @Override
    public void onRemove(long id, BufferHolder map) {
        bitSet.set((int)(id/StoreConfig.LOG_ELEMNT_LEAST_SIZE),false);
    }

    @Override
    public void onCache(long id, BufferHolder map) {
        bitSet.set((int)(id/StoreConfig.LOG_ELEMNT_LEAST_SIZE));
    }

    @Override
    public String toString() {
        return super.toString();
    }
    public List<Long> topMiss(int n){

        return null;
    }

}
