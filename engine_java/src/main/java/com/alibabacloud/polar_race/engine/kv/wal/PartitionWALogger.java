package com.alibabacloud.polar_race.engine.kv.wal;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Memory;
import com.alibabacloud.polar_race.engine.kv.buffer.LogBufferAllocator;
import com.alibabacloud.polar_race.engine.kv.cache.KVCacheController;
import com.alibabacloud.polar_race.engine.kv.event.Put;
import com.alibabacloud.polar_race.engine.kv.event.TaskBus;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.file.LogFileServiceImpl;
import com.alibabacloud.polar_race.engine.kv.partition.LexigraphicalPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.concurrent.*;


/**
 *
 * 支持多partition
 *
 *
 **/
public class PartitionWALogger  extends Service implements WALog<Put> {
    private final static Logger logger= LoggerFactory.getLogger(PartitionWALogger.class);
    private ExecutorService commonExecutorService;
    private LexigraphicalPartition partitioner;
    private String parentDir;
    private WALogger[] partitionWALoggers;
    private LogFileService partitionLogFileService;
    private TaskBus fileChannelCloseProcessor;
    private KVCacheController cacheController;
    private LogBufferAllocator bufferAllocator;
    private IndexServiceManager indexServiceManager;
    public PartitionWALogger(String parent){
        this.parentDir=parent;
        this.partitioner=new LexigraphicalPartition(Long.MIN_VALUE,Long.MAX_VALUE, StoreConfig.PARTITIONS,1);
        this.partitionWALoggers=new WALogger[partitioner.size()];
        this.partitionLogFileService =new LogFileServiceImpl(parentDir,fileChannelCloseProcessor);
        this.cacheController=new KVCacheController(partitionLogFileService);
        this.fileChannelCloseProcessor=new TaskBus(StoreConfig.WRITE_HANDLER_CLOSE_PROCESSOR);
        //this.bufferAllocator=new LogBufferAllocator(partitionLogFileService,maxDirectCacheLog,maxHeapCacheLog,cacheController.maxDirectBuffer(),cacheController.maxOldBuffer())
        this.bufferAllocateControl();
        this.commonExecutorService = new ThreadPoolExecutor(Math.min(cacheController.cacheIndexInitLoadConcurrency(),cacheController.cacheLogInitLoadConcurrency()),
                Math.max(cacheController.cacheIndexInitLoadConcurrency(),cacheController.cacheLogInitLoadConcurrency()),
                60, TimeUnit.SECONDS,new LinkedBlockingQueue<>());
        this.indexServiceManager=new IndexServiceManager(parentDir);
    }

    /**
     * 控制整体项目的缓存
     **/
    public void bufferAllocateControl(){
        int maxDirectCacheLog=cacheController.maxLogCacheDirectBuffer()/cacheController.cacheLogSize();
        int maxHeapCacheLog=cacheController.maxCacheLog()-maxDirectCacheLog+2* StoreConfig.MAX_CONCURRENCY_PRODUCER_AND_CONSUMER;
        logger.info(String.format("max cache put file direct %d, heap %d",maxDirectCacheLog,maxHeapCacheLog));
        this.bufferAllocator=new LogBufferAllocator(partitionLogFileService,maxDirectCacheLog,maxHeapCacheLog,cacheController.maxDirectBuffer(),cacheController.maxOldBuffer());
    }

    @Override
    public void onStart() throws Exception {
        // partitionId
        String partitionDir;
        CountDownLatch startLatch=new CountDownLatch(partitioner.size());
        for(int i=0;i<partitioner.size();i++){
              partitionDir=parentDir+String.format("%d/",i);
              partitionWALoggers[i]=new WALogger(partitionDir,commonExecutorService,bufferAllocator,partitioner.getPartition(i),indexServiceManager);

        }
        indexServiceManager.start(); // for sequence index service
        for (int i=0;i<partitioner.size();i++){
            new Thread(new BootStrapWALoggerTask(i,startLatch)).start();
        }
        startLatch.await(); // 直到启动完成
        commonExecutorService.shutdown(); // close thread pool
        if(commonExecutorService.awaitTermination(1, TimeUnit.SECONDS)){
            logger.info(" index and put cache finish");
        }else{
            logger.info(" index and put cache timeout,continue");
        }
        logger.info(Memory.memory().toString());
        logger.info("start all partition ");
    }



    @Override
    public long log(Put cell) throws Exception {
        return 0;
    }

    @Override
    public void iterate(AbstractVisitor visitor) throws IOException {

    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
           partitioner.iterate(Bytes.bytes2long(lower,0),Bytes.bytes2long(upper,0),visitor);
    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        int partitionId=partitioner.partition(Bytes.bytes2long(key,0));
        return partitionWALoggers[partitionId].get(key);
    }

    @Override
    public long put(byte[] key, byte[] value) throws Exception {
        int partitionId=partitioner.partition(Bytes.bytes2long(key,0));
       return partitionWALoggers[partitionId].put(key,value);
    }

    /**
     *
     **/
    public class BootStrapWALoggerTask implements Runnable{
        private int partitionId;
        private CountDownLatch startLatch;
        public BootStrapWALoggerTask(int partitionId, CountDownLatch latch){
            this.partitionId=partitionId;
            this.startLatch=latch;
        }
        @Override
        public void run() {
            try {
                partitionWALoggers[partitionId].start(); // bocking until finish
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                startLatch.countDown();
            }
        }
    }

    @Override
    public void onStop() throws Exception {
//        super.onStop();
        this.fileChannelCloseProcessor.stop();
        for(WALogger partition:partitionWALoggers){
            partition.stop();
        }
        partitioner.close();
    }
}
