package com.alibabacloud.polar_race.engine.common;

public class StoreConfig {
    public final static int SEGMENT_LOG_FILE_SIZE =512*1024;
    public final static int  EMPTY_FILL_BUFFER_SIZE =8*1024;
    public final static String LOG_FILE_SUFFIX=".wal";
    public final static int  FILE_WRITE_BUFFER_SIZE =64*1024;
    public final static int  FILE_READ_BUFFER_SIZE=16*1024*1024;
    public final static int  DISRUPTOR_BUFFER_SIZE=1024;
    public final static int  KEY_SIZE=8;
    public final static int  VALUE_SIZE=4096;
    public final static int  VALUE_INDEX_RECORD_SIZE=16;
    public final static int  K4_SIZE=4096;
    public final static int  KEY_INDEX_MAP_INIT_CAPACITY=100000;
    public final static int  MAXIMUM_BUFFER_CAPACITY =256*1024*1024;
    public final static int  batchSyncSize =56;
    public final static int  LOG_KV_RECORD_LEAST_LEN=2;
    public final static int  LONG_LEN=8;
    public final static int  INT_LEN=4;
    public final static int  SHORT_LEN=2;
    public final static int  MAX_TIMEOUT=1000;
    public final static byte  verison=(byte) 1;

}
