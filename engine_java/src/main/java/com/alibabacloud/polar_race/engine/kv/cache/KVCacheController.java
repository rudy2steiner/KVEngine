package com.alibabacloud.polar_race.engine.kv.cache;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.kv.LogFileService;

public class KVCacheController implements CacheController {
    private LogFileService logFileService;
    public KVCacheController(LogFileService logFileService){
        this.logFileService=logFileService;
    }
    @Override
    public int maxDirectBuffer() {
        return StoreConfig.MAX_DIRECT_BUFFER_SIZE;
    }

    @Override
    public int maxOldBuffer() {
        return StoreConfig.MAX_OLD_BUFFER_SIZE;
    }

    @Override
    public int maxCacheLog() {
        // 1200* 512kb  =600Mb
        return 1200;
    }

    @Override
    public double cacheLogLoadFactor() {
        return 0.5;
    }

    @Override
    public int cacheLogInitLoadConcurrency() {
        return 64;
    }

    @Override
    public int cacheLogSize() {
        return logFileService.logWritableSize();
    }

    @Override
    public double cacheIndexLoadFactor() {
        return 0.5;
    }

    @Override
    public int cacheIndexReadBufferSize() {
        return 256*1024;  //256Kb
    }

    @Override
    public int cacheIndexInitLoadConcurrency() {
        // 64*256kb= 16Mb
        return maxHashBucketSize();
    }

    @Override
    public int maxCacheIndex() {
              // 12* 16Mb= 192Mb
        return 56;
    }

    @Override
    public int maxHashBucketSize() {
        return StoreConfig.HASH_BUCKET_SIZE;
    }

    @Override
    public int cacheIndexSize() {
        // suppose 均匀分布在各桶

        return 16*1024*1024; // 16Mb long log map
    }

    @Override
    public int logElementLeastSize() {
        return 0;
    }
}
