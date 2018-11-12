package com.alibabacloud.polar_race.engine.kv.cache;


import com.alibabacloud.polar_race.engine.common.collection.LongSet;
import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.utils.Null;
import com.alibabacloud.polar_race.engine.kv.buffer.BufferHolder;
import com.alibabacloud.polar_race.engine.kv.event.CacheEvent;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


/**
 *  blocking queue lock lead performance very badly
 *
 **/
public class LogCacheMonitor extends Service implements CacheListener<BufferHolder> {
    private final static Logger logger= LoggerFactory.getLogger(LogCacheMonitor.class);
    private long   TIMOUT=100;
    private BitSet inMemory;
    private volatile short[] missStat;
    /**
     * on load file id
     * */
    private LongSet onLoad;
    /**
     *  运行过程中整体的缓存失效次数统计
     **/
    private  int[] missCount;
    private AtomicInteger[] atomicMissCount;
    private LogFileService logFileService;
    private BlockingQueue<CacheEvent> cacheEvents;
    private Thread  eventProcessorThread;
    private CacheEventProcessor processor;
    private LoadingCache<Long, BufferHolder> lru;
    private LogFileLRUCache logLRU;
    public LogCacheMonitor(LogFileService logFileService,LoadingCache<Long, BufferHolder> logCache,LogFileLRUCache logLRU){
        this.logFileService=logFileService;
        this.inMemory =new BitSet(logFileService.allLogFiles().size());
        this.missStat=new short[logFileService.allLogFiles().size()];
        this.missCount=new int[logFileService.allLogFiles().size()];
        this.atomicMissCount=new AtomicInteger[logFileService.allLogFiles().size()];
        this.cacheEvents=new LinkedBlockingQueue();
        this.lru=logCache;
        this.logLRU=logLRU;
        this.onLoad= LongSet.withExpectedSize(1000);
        this.processor=new CacheEventProcessor();
        this.eventProcessorThread=new Thread(processor);
    }
    /**
     *
     **/
    public void put(CacheEvent event) throws InterruptedException{
       // cacheEvents.put(event);
    }
    @Override
    public void onMissCache(long id) {
        int missIndex=(int)(id/StoreConfig.SEGMENT_LOG_FILE_SIZE);
        missStat[missIndex]+=1;
       // atomicMissCount[missIndex].incrementAndGet();

    }

    @Override
    public void onRemove(long id, BufferHolder bufferHolder) {
        inMemory.set((int)(id/StoreConfig.SEGMENT_LOG_FILE_SIZE),false);
    }

    @Override
    public void onCache(long id, BufferHolder bufferHolder) {
        // put cache
        lru.put(id,bufferHolder);
        onLoad.removeLong(id);
        int missIndex=(int)(id/StoreConfig.SEGMENT_LOG_FILE_SIZE);
        inMemory.set(missIndex);
        missStat[missIndex]=0; // miss state clear

    }

    @Override
    public String toString() {
        return "unsupport";//Arrays.toString(topK(Math.min(1000,logFileService.allLogFiles().size())));
    }
    public AtomicInteger[] topK(int k){
        Arrays.sort(atomicMissCount, new Comparator<AtomicInteger>() {
            @Override
            public int compare(AtomicInteger o1, AtomicInteger o2) {
                return o2.get()-o1.get();
            }
        });
        logger.info(Arrays.toString(atomicMissCount));
        return Arrays.copyOfRange(atomicMissCount,0,k);
    }

    @Override
    public void onStart() throws Exception {
//         processor.start();
//         eventProcessorThread.start();
        Arrays.fill(atomicMissCount,new AtomicInteger(0));
    }

    @Override
    public void onStop() throws Exception {
        logger.info("top miss:"+toString());
//        processor.stop();
    }


    /**
     * cache event processor
     **/
    public class CacheEventProcessor extends Service implements Runnable{

        private  int cacheLoadCount=0;
        private CacheEvent event;
        @Override
        public void run() {
             while (this.isStarted()){
                 try {
                     event = cacheEvents.poll(TIMOUT, TimeUnit.MILLISECONDS);
                     if(Null.isEmpty(event)){
                         logger.info("cache event processor timeout,sleep 10ms");
                         Thread.sleep(10);
                         // possible close
                         continue;
                     }
                     switch (event.type()){
                         case CACHE_OUT:
                             onRemove(event.value(),null);
                             break;
                         case CACHE_MISS:
                             onMissCache(event.value());
                             loadFrequencyLogFileCache(event);
                             break;
                         default:

                     }
                 }catch (InterruptedException e){
                     logger.info("cache event processor interrupted!");
                 }
             }
            logger.info("cache event processor stopped!");
        }

        /**
         *
         * 提交加载log cache task
         **/
        public void loadFrequencyLogFileCache(CacheEvent cacheEvent){
            int missIndex=(int)(cacheEvent.value()%StoreConfig.SEGMENT_LOG_FILE_SIZE);
             if(missStat[missIndex]>StoreConfig.CACHE_LOG_MISS_LIMIT&&!inMemory.get(missIndex)
                                                &&!onLoad.contains(event.value().longValue())){
                 onLoad.add(event.value().longValue());
                 logLRU.loadCache(event.value());
                 if(cacheLoadCount++%1000==0){
                     logger.info(String.format("log file load task scheduled %d",event.value()));
                 }
             }
        }

    }



}
