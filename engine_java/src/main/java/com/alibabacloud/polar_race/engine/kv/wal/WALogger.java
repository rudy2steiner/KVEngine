package com.alibabacloud.polar_race.engine.kv.wal;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.kv.*;
import com.alibabacloud.polar_race.engine.kv.buffer.LogBufferAllocator;
import com.alibabacloud.polar_race.engine.kv.cache.CacheController;
import com.alibabacloud.polar_race.engine.kv.cache.IndexLRUCache;
import com.alibabacloud.polar_race.engine.kv.cache.KVCacheController;
import com.alibabacloud.polar_race.engine.kv.cache.LogFileLRUCache;
import com.alibabacloud.polar_race.engine.kv.event.Put;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.file.LogFileServiceImpl;
import com.alibabacloud.polar_race.engine.kv.index.IndexHashAppender;
import com.alibabacloud.polar_race.engine.kv.index.IndexLogReader;
import com.alibabacloud.polar_race.engine.kv.index.IndexVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class WALogger implements WALog<Put> {
    private final static Logger logger= LoggerFactory.getLogger(WALogger.class);
    private ThreadLocal<ByteBuffer> valueBuf=new ThreadLocal<>();
    private String walDir;
    private String indexDir;
    private String rootDir;
    private LogFileService logFileService;
    private LogFileService indexFileService;
    private MultiTypeLogAppender appender;
    private IndexLogReader indexLogReader;
    private IndexHashAppender hashIndexAppender;
    private CacheController cacheController;
    private IndexLRUCache indexLRUCache;
    private LogFileLRUCache logFileLRUCache;
    private ExecutorService commonExecutorService;
    private LogBufferAllocator bufferAllocator;
    private CountDownLatch latch;
    public WALogger(String dir){
        this.rootDir=dir;
        this.walDir =dir+StoreConfig.VALUE_CHILD_DIR;
        this.indexDir=dir+StoreConfig.INDEX_CHILD_DIR;
        Files.makeDirIfNotExist(walDir);
        // empty index ,每次起来都重建hash桶
        Files.emptyDirIfExist(indexDir);
        Files.makeDirIfNotExist(indexDir);
        this.logFileService =new LogFileServiceImpl(walDir);
        this.indexFileService=new LogFileServiceImpl(indexDir);
        this.cacheController=new KVCacheController(logFileService);
        this.bufferAllocateControl();
        this.commonExecutorService = new ThreadPoolExecutor(Math.min(cacheController.cacheIndexInitLoadConcurrency(),cacheController.cacheLogInitLoadConcurrency()),
                                                     Math.max(cacheController.cacheIndexInitLoadConcurrency(),cacheController.cacheLogInitLoadConcurrency()),
                                        60, TimeUnit.SECONDS,new LinkedBlockingQueue<>());
        this.indexLRUCache=new IndexLRUCache(cacheController,indexFileService, commonExecutorService,bufferAllocator);
        this.logFileLRUCache=new LogFileLRUCache(logFileService,cacheController, commonExecutorService,bufferAllocator);
        this.transferIndexLogToHashBucketInit();

    }


    /**
     * 控制整体项目的缓存
     **/
    public void bufferAllocateControl(){
        int maxDirectCacheLog=cacheController.maxLogCacheDirectBuffer()/cacheController.cacheLogSize();
        int maxHeapCacheLog=cacheController.maxCacheLog()-maxDirectCacheLog+2* StoreConfig.MAX_CONCURRENCY_PRODUCER_AND_CONSUMER;
        logger.info(String.format("max direct cache log file %d, heap %d",maxDirectCacheLog,maxHeapCacheLog));
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
            WalLogParser logParser=new WalLogParser(logFileService,lastLogName+StoreConfig.LOG_FILE_SUFFIX);
             ByteBuffer to=bufferAllocator.allocate(logFileService.logWritableSize(),false);
             handler=logParser.doRecover(null,to,null);
             bufferAllocator.onRelease(to);
        }
        return handler;
    }


    /**
     *  启动hash 到索引文件引擎
     **/

    public boolean startAsyncHashBucketTask() throws Exception{
        if(logFileService.allLogFiles().size()>0) {
            this.latch=new CountDownLatch(1);
            long start=System.currentTimeMillis();
            hashIndexAppender.start();
            indexLogReader.start();
            indexLogReader.iterate(new IndexVisitor() {
                private AtomicInteger concurrency=new AtomicInteger(cacheController.maxHashBucketSize());
                @Override
                public void visit(ByteBuffer buffer) throws Exception {
                    hashIndexAppender.append(buffer);
                }
                @Override
                public void onFinish() throws Exception{
                    if(concurrency.decrementAndGet()==0) {
                        hashIndexAppender.close();
                        latch.countDown();
                        logger.info(String.format("hash task  finish, time %d ms",System.currentTimeMillis()-start));

                    }
                }
            },cacheController.maxHashBucketSize());
            return true;
        }
        return false;
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
            long offset= indexLRUCache.getOffset(expectedKey);
            if(offset<0){
                throw new EngineException(RetCodeEnum.NOT_FOUND,"not found "+expectedKey);
            }
            ByteBuffer valueBuffer=valueBuf.get();
                if( valueBuffer==null){
                    valueBuffer=bufferAllocator.allocate(StoreConfig.VALUE_SIZE,false);
                    valueBuf.set(valueBuffer);
                }
                valueBuffer.clear();
                logFileLRUCache.readValue(expectedKey,offset,valueBuffer);
        return  valueBuffer.array();
    }

    @Override
    public void start() throws Exception{
        IOHandler handler;
        String nextLogName;
        boolean redo=logFileService.needReplayLog();
        if(redo){
            handler=replayLastLog();
            nextLogName= logFileService.nextLogName(handler);
        }else{
            nextLogName= logFileService.nextLogName();
        }
        // ensure hash bucket task is finished,possible optimize
        if(startAsyncHashBucketTask()) {
            latch.await();
            indexLRUCache.start();
            logFileLRUCache.start();
        }
        commonExecutorService.shutdown();
        if(commonExecutorService.awaitTermination(1000, TimeUnit.MILLISECONDS)){
            logger.info("hash bucket and load  index,log cache finish");
        }else{
            logger.info("hash bucket and load  index,log cache timeout,continue");
        }
        handler= logFileService.bufferedIOHandler(nextLogName,StoreConfig.FILE_WRITE_BUFFER_SIZE);
        this.appender=new MultiTypeLogAppender(handler, logFileService,StoreConfig.DISRUPTOR_BUFFER_SIZE);
        this.appender.start();
        onStartFinish();
    }


    /**
     *  only 启动 hash  engine
     **/
    public void startAsyncHashTask() throws Exception{
        boolean redo=logFileService.needReplayLog();
        if(redo){
            replayLastLog();
        }
        startAsyncHashBucketTask();
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
        if(startAsyncHashBucketTask()) {
            // 等hash 完成
            latch.await();
            indexLRUCache.iterateKey();
        }
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
        logger.info("wal logger started");

    }

    /**
     * */
    public void transferIndexLogToHashBucketInit(){
        hashIndexAppender=new IndexHashAppender(indexDir,cacheController.maxHashBucketSize(),cacheController.hashBucketWriteCacheSize());
        indexLogReader=new IndexLogReader(walDir,logFileService, commonExecutorService);
    }

    @Override
    public void close() throws Exception {
         this.appender.close();
         this.hashIndexAppender.close();
         this.indexLRUCache.close();
         this.logFileLRUCache.close();
         logger.info("close wal logger");
    }


}
