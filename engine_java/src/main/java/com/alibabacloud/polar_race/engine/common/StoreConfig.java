package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.kv.event.ReadType;

public class StoreConfig {
    public final static int  SEGMENT_LOG_FILE_SIZE =2*1024*1024;
    public final static long  G=1024*1024*1024; // 256G

    public final static long  MAX_LOG_SPACE_SIZE=(64*4*+1)+1024*1024*1024; // 256G
    public final static int  EMPTY_FILL_BUFFER_SIZE =8*1024;
    public final static String LOG_FILE_SUFFIX=".wal";
    public final static String VALUE_CHILD_DIR="wal/";
    public final static String INDEX_CHILD_DIR="index/";
    public final static String ORDERED_LOG_CHILD_DIR ="order/"; //顺序日志存储子目录
    public final static String LOG_INDEX_FILE_SUFFIX=".index";
    public final static int  HASH_INDEX_QUEUE_SIZE=500;
    public final static double  HASH_BUCKET_LOAD_FACTOR =0.5;
    public final static int  HASH_WRITE_BUFFER_SIZE =256*1024;
    public final static int  HASH_CONCURRENCY =32;

    public final static int  LOAD_HASH_INDEX_TIMEOUT =2000;
    public final static int  HASH_LOAD_BUFFER_SIZE =256*1024;
    public final static int  FILE_WRITE_BUFFER_SIZE =128*1024;
    public final static int  FILE_READ_BUFFER_SIZE=16*1024*1024;
    public final static int  DISRUPTOR_BUFFER_SIZE=1024;
    public final static int  KEY_SIZE=8;
    public final static int  VALUE_SIZE=4096;
    public final static int  KEY_VALUE_SIZE=KEY_SIZE+VALUE_SIZE;
    public final static int  VALUE_INDEX_RECORD_SIZE=16;
    public final static int  K4_SIZE=4096;
    public final static int  KEY_INDEX_MAP_INIT_CAPACITY=100000;
    public final static int  MAXIMUM_BUFFER_CAPACITY =256*1024*1024;
    /**
     *  hash bucket
     **/
    public final static int  HASH_BUCKET_SIZE =16; // 2^n
    public final static int  PARTITIONS =12;         //wal 分区数
    public final static int  batchSyncSize_FLUCTUATE =1;
    public final static int  batchSyncSize =6;
    public final static int  MINI_batchSyncSize =5;
    public final static int  BATCH_IOHANDLER =100;
    public final static long  ORDER_LOG_TRANSFER_DISK_SIZE =50*G;


    public final static int  LOG_KV_RECORD_LEAST_LEN=2;
    public final static int  LONG_LEN=8;
    public final static int  INT_LEN=4;
    public final static int  SHORT_LEN=2;
    public final static int LOG_ELEMENT_LEAST_SIZE =SHORT_LEN+KEY_SIZE;
    public final static int  MAX_TIMEOUT=1000;
    public final static byte VERSION =(byte) 1;
    public final static int  MAX_DIRECT_BUFFER_SIZE=200*1024*1024; // 220Mb
    public final static int  MAX_OLD_BUFFER_SIZE=750*1024*1024;  // 750Mb  full gc limit
    public final static int  MAX_CONCURRENCY_PRODUCER_AND_CONSUMER=64;
    public final static float  TROVE_LOAD_FACTOR=0.98f;
    public final static int   WRITE_HANDLER_CLOSE_PROCESSOR=2;
    public final static int   PAGE_CACHE_LIMIT=1560;  //MB
    public final static int  LOG_ELEMENT_SIZE=VALUE_SIZE+KEY_SIZE+SHORT_LEN;
    public final static long   STORE_TIMEOUT=1200;  //second

    public final static ReadType startType=ReadType.RANGE;
    /**
     *  失败 太多，直接cache 线程加载到缓存
     * */
    public final static int  CACHE_LOG_MISS_LIMIT=5;
    /***
     *  time elapsed 75355 ms,qps 84931, write buffer 128k,batch size 16,10w per thread
     *   time elapsed 74864 ms,qps 85488 ,write buffer 128k,batch size 8,10w per thread
     *   time elapsed 75707 ms,qps 84536 ,write buffer 128k,batch size 4,10w per thread
     **/
}
