package com.alibabacloud.polar_race.engine.kv.cache;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.kv.buffer.BufferHolder;
import com.alibabacloud.polar_race.engine.kv.wal.WalReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogLoadCacheTask implements Runnable{
    private final static Logger logger= LoggerFactory.getLogger(LogLoadCacheTask.class);
    private WalReader reader;
    // without suffix
    private long fileName;
    private CacheListener<BufferHolder> logCacheListener;
    private LogFileLRUCache logLRU;
    private CacheController cacheController;
    public LogLoadCacheTask(WalReader reader,long fileName,CacheController cacheController,CacheListener<BufferHolder> logCacheListener,LogFileLRUCache logFileLRUCache){
        this.reader=reader;
        this.fileName=fileName;
        this.logCacheListener=logCacheListener;
        this.cacheController=cacheController;
        this.logLRU=logFileLRUCache;
    }
    @Override
    public void run() {
        try {
            BufferHolder holder=logLRU.getLogBufferHolder();
            int cacheLogSize=cacheController.cacheLogSize();
            reader.read(fileName+ StoreConfig.LOG_FILE_SUFFIX,holder.value() , cacheLogSize);
            holder.value().flip();
            // cache
            //lru.put(fileName,holder);
            //logCacheListener.onCache(fileName,holder);
        }catch (Exception  e){
            logger.info("read log to cache failed",e);
        }
    }

}
