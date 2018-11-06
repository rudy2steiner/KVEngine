package com.alibabacloud.polar_race.engine.kv.cache;

public interface CacheController {


    /**
     * 最大可分配的直接内存
     **/
    int maxDirectBuffer();


    /**
     * 最大可分配的直接内存
     **/
    int maxLogCacheDirectBuffer();

    /**
     * 最大可分配的Heap old 内存
     **/
    int maxOldBuffer();
    /**
     *  最大cache log 的数量
     **/
     int maxCacheLog();

     /**
      *  初始化装载因子
      **/
     double cacheLogLoadFactor();

    /**
     *
     *  初始化LRU  cache log 的并行度
     * */
    int  cacheLogInitLoadConcurrency();
    /**
     *  每个cache log 的大小
     **/
    int cacheLogSize();


    /**
     *  初始化装载因子
     **/
    double cacheIndexLoadFactor();

    /**
     *
     *  读取cache index 的缓存块大小
     * */
    int  cacheIndexReadBufferSize();

    /**
     *
     *  初始化LRU  cache index 的并行度
     * */
    int  cacheIndexInitLoadConcurrency();

     /**
      *
      * 最大cache index 的数量
      **/
     int maxCacheIndex();

    /**
     * 最大的hash 桶文件大小
     **/
    int maxHashBucketSize();


    /**
     * 最大的hash 桶文件大小
     **/
    int hashBucketWriteCacheSize();
    /**
     *  每个cache index 的大小
     **/
    int cacheIndexSize();

    int logElementLeastSize();



}
