package com.alibabacloud.polar_race.engine.common;

public class StoreConfig {
    public final static long SEGMENT_FILE_SIZE=32*1024*1024;
    public final static int  empty_fill_buffer_size=8*1024;
    public final static String LOG_FILE_SUFFIX=".wal";
    public final static int FILE_WRITE_BUFFER_SIZE =8*1024*1024;
    public final static int  FILE_READ_BUFFER_SIZE=8*1024*1024;
    public final static int  DISRUPTOR_BUFFER_SIZE=8*1024;
    public final static int  KEY_SIZE=8;
    public final static int  VALUE_SIZE=4096;
}
