package com.alibabacloud.polar_race.engine.kv.wal;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.common.utils.Memory;
import com.alibabacloud.polar_race.engine.common.utils.Null;
import com.alibabacloud.polar_race.engine.kv.*;
import com.alibabacloud.polar_race.engine.kv.buffer.LogBufferAllocator;
import com.alibabacloud.polar_race.engine.kv.cache.*;
import com.alibabacloud.polar_race.engine.kv.event.TaskBus;
import com.alibabacloud.polar_race.engine.kv.event.Put;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.file.LogFileServiceImpl;
import com.alibabacloud.polar_race.engine.kv.index.IndexService;
import com.alibabacloud.polar_race.engine.kv.index.KVIndexService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class WALogger extends Service implements WALog<Put> {
    private final static Logger logger= LoggerFactory.getLogger(WALogger.class);
    private ThreadLocal<ByteBuffer> valueBuf=new ThreadLocal<>();
    private String walDir;
    private String indexDir;
    private String rootDir;
    private LogFileService logFileService;
    private LogFileService indexFileService;
    private MultiTypeLogAppender appender;
    private CacheController cacheController;
    private LogFileLRUCache logFileLRUCache;
    private ExecutorService commonExecutorService;
    private LogBufferAllocator bufferAllocator;
    private TaskBus fileChannelCloseProcessor;
    private IOHandlerLRUCache logHandlerLRUCache;
    private AtomicInteger readCounter=new AtomicInteger(0);
    private IndexService indexService;
    private ScheduledExecutorService timer=Executors.newScheduledThreadPool(1);
    private Status storeStatus;
    public WALogger(String dir){
        this.rootDir=dir;
        this.walDir =dir+StoreConfig.VALUE_CHILD_DIR;
        this.indexDir=dir+StoreConfig.INDEX_CHILD_DIR;
        Files.makeDirIfNotExist(walDir);
        // empty index ,每次起来都重建hash桶
        Files.emptyDirIfExist(indexDir);
        Files.makeDirIfNotExist(indexDir);
        this.fileChannelCloseProcessor=new TaskBus(StoreConfig.WRITE_HANDLER_CLOSE_PROCESSOR);
        this.logFileService =new LogFileServiceImpl(walDir,fileChannelCloseProcessor);
        this.indexFileService=new LogFileServiceImpl(indexDir,fileChannelCloseProcessor);
        this.cacheController=new KVCacheController(logFileService);
        this.bufferAllocateControl();
        this.commonExecutorService = new ThreadPoolExecutor(Math.min(cacheController.cacheIndexInitLoadConcurrency(),cacheController.cacheLogInitLoadConcurrency()),
                                                     Math.max(cacheController.cacheIndexInitLoadConcurrency(),cacheController.cacheLogInitLoadConcurrency()),
                                        60, TimeUnit.SECONDS,new LinkedBlockingQueue<>());
        this.storeStatus=Status.START;
    }


    /**
     * 控制整体项目的缓存
     **/
    public void bufferAllocateControl(){
        int maxDirectCacheLog=cacheController.maxLogCacheDirectBuffer()/cacheController.cacheLogSize();
        int maxHeapCacheLog=cacheController.maxCacheLog()-maxDirectCacheLog+2* StoreConfig.MAX_CONCURRENCY_PRODUCER_AND_CONSUMER;
        logger.info(String.format("max cache log file direct %d, heap %d",maxDirectCacheLog,maxHeapCacheLog));
        this.bufferAllocator=new LogBufferAllocator(logFileService,maxDirectCacheLog,maxHeapCacheLog,cacheController.maxDirectBuffer(),cacheController.maxOldBuffer());
    }


    /**
     *  查看是否异常退出，并恢复日志完整性
     *  单线程，恢复最后一个日志文件
     *
     **/
    public IOHandler replayLastLog() throws IOException{
        String lastLogName= logFileService.lastLogName();
        IOHandler handler=null;
        if(lastLogName!=null){
            storeStatus=Status.CORRUPTED;
            WalLogParser logParser=new WalLogParser(logFileService,lastLogName+StoreConfig.LOG_FILE_SUFFIX);
             ByteBuffer to=bufferAllocator.allocate(logFileService.logWritableSize(),false);
             handler=logParser.doRecover(null,to,null);
             bufferAllocator.onRelease(to);
            storeStatus=Status.READ;
        }
        return handler;
    }
    @Override
    public long log(Put event) throws Exception{
           appender.append(event);
           return event.txId();
    }




    @Override
    public void iterate(AbstractVisitor visitor) throws IOException {
        List<Long> logNames= logFileService.allLogFiles();
        LogParser parser;
        ByteBuffer to= bufferAllocator.allocate(StoreConfig.FILE_READ_BUFFER_SIZE,false);
        ByteBuffer from= bufferAllocator.allocate(StoreConfig.FILE_READ_BUFFER_SIZE,false);
        for(Long logName:logNames){
            parser=new LogParser(walDir,logName+StoreConfig.LOG_FILE_SUFFIX);
            parser.parse(visitor,to,from);
            to.clear();
            from.clear();
        }
    }


    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) {

    }
    @Override
    public byte[] get(final byte[] key) throws Exception{
            long expectedKey=Bytes.bytes2long(key,0);
            int offset= indexService.getOffset(expectedKey);
            if(offset<0){
                throw new EngineException(RetCodeEnum.NOT_FOUND,"not found "+expectedKey);
            }
            ByteBuffer valueBuffer=valueBuf.get();
                if( valueBuffer==null){
                    valueBuffer=bufferAllocator.allocate(StoreConfig.VALUE_SIZE,false);
                    valueBuf.set(valueBuffer);
                    logger.info(String.format("allocate buffer for %d",Thread.currentThread().getId()));
                }
                valueBuffer.clear();
                logFileLRUCache.readValueIfCacheMiss(expectedKey,offset,valueBuffer);
        if(readCounter.incrementAndGet()%1000000==0){
            logger.info(Memory.memory().toString());
        }
        valueBuffer.flip();
        return  valueBuffer.array();
    }

    @Override
    public void onStart() throws Exception{
        IOHandler handler;
        String nextLogName;
        // start to process io handler close
        fileChannelCloseProcessor.start();
        boolean redo=logFileService.needReplayLog();
        if(redo){
            handler=replayLastLog();
            nextLogName= logFileService.nextLogName(handler);
        }else{
            storeStatus=Status.NORMAL_EXIT;
            nextLogName= logFileService.nextLogName();
        }
        // ensure hash bucket task is finished,possible optimize,log 文件已存在，加载索引
        if(logFileService.allLogFiles().size()>0) {
            // 依据store 的状态，看是否需要加载缓存
            logHandlerLRUCache=new IOHandlerLRUCache(logFileService);
            logHandlerLRUCache.start();
            indexService=new KVIndexService(indexDir,walDir,cacheController,fileChannelCloseProcessor,bufferAllocator,logFileService,
                                           indexFileService,logHandlerLRUCache,commonExecutorService);
            ((KVIndexService) indexService).start();
            logFileLRUCache=new LogFileLRUCache(logFileService,logHandlerLRUCache,cacheController, commonExecutorService,bufferAllocator);
            //indexLRUCache.start();
            logFileLRUCache.start();
        }else{
            logger.info("log and index cache engine start ignore");
        }
        statisticsLogAndHashIndex();
        commonExecutorService.shutdown();
        if(commonExecutorService.awaitTermination(10, TimeUnit.SECONDS)){
            logger.info(" index and log cache finish");
        }else{
            logger.info(" index and log cache timeout,continue");
        }
        handler= logFileService.bufferedIOHandler(nextLogName,StoreConfig.FILE_WRITE_BUFFER_SIZE);
        this.appender=new MultiTypeLogAppender(handler, logFileService,StoreConfig.DISRUPTOR_BUFFER_SIZE);
        this.appender.start();
        onStartFinish();
    }

    /**
     * 统计文件下索引和日志数量及占用空间
     **/
    public void statisticsLogAndHashIndex(){
        int  indexFiles=indexFileService.allSortedFiles(StoreConfig.LOG_INDEX_FILE_SUFFIX).size();
        long indexTotal=indexFileService.addByteSize(0l);
        long logTotal=logFileService.lastWriteLogName(true);// 不准确
        logger.info(String.format("index file %d,total size %d ;log file total %d",indexFiles,indexTotal,logTotal));
    }




    /**
     *  only 启动 hash  engine
     **/
    public void startAsyncHashTask() throws Exception{
        boolean redo=logFileService.needReplayLog();
        if(redo){
            replayLastLog();
        }
        commonExecutorService.shutdown();
        if(commonExecutorService.awaitTermination(10, TimeUnit.SECONDS)){
            logger.info("hash index finished");
        }else{
            logger.info("hash index timeout,continue");
        }
    }

    /**
     *  only 启动 index  engine
     **/
    public void startAsyncIndexCacheTask() throws Exception{
        boolean redo=logFileService.needReplayLog();
        if(redo) {
          replayLastLog();
        }
//        if(startAsyncHashBucketTask()) {
//            // 等hash 完成
//            latch.await();
//            indexLRUCache.iterateKey();
//        }
        commonExecutorService.shutdown();
        if(commonExecutorService.awaitTermination(10, TimeUnit.SECONDS)){
            logger.info(" index and log cache finish");
        }else{
            logger.info(" index and log cache timeout,continue");
        }
    }

    /**
     * to do release if need
     * */
    public void onStartFinish(){
        logger.info(String.format("wal logger started,path %s",rootDir));
        timer.schedule(new StoreGuardTimeout(),StoreConfig.STORE_TIMEOUT,TimeUnit.SECONDS);
    }

    public class StoreGuardTimeout implements Runnable{
        @Override
        public void run() {
            logger.info("timeout");
            try {
                stop();
            }catch (Exception e){
                logger.info("time stop exception",e);
            }
        }
    }

    @Override
    public void onStop() throws Exception {
        long start=System.currentTimeMillis();
         logger.info(Memory.memory().toString());
         this.appender.stop();
        if(!Null.isEmpty(logFileLRUCache))
            this.logFileLRUCache.stop();
         this.fileChannelCloseProcessor.stop();
         if(!Null.isEmpty(logHandlerLRUCache))
             logHandlerLRUCache.stop();
         this.timer.shutdownNow();
         logger.info(Memory.memory().toString());
         statisticsLogAndHashIndex();
         logger.info("asyncClose wal logger,close time elapsed "+(System.currentTimeMillis()-start));
    }

    enum Status{
         START,CORRUPTED,NORMAL_EXIT,READ,WRITE
    }


}
