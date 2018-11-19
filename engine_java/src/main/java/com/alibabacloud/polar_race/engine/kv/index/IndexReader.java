package com.alibabacloud.polar_race.engine.kv.index;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Null;
import com.alibabacloud.polar_race.engine.kv.cache.CacheListener;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongLongHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexReader {
    private final static Logger logger= LoggerFactory.getLogger(IndexReader.class);
    private static AtomicInteger keyCounter=new AtomicInteger(0);
    private static AtomicInteger readBucketCounter=new AtomicInteger(0);
    private static AtomicInteger duplicatedCounter=new AtomicInteger(0);
    public LogFileService indexFileService;
    public IndexReader(LogFileService indexFileService){
        this.indexFileService=indexFileService;
    }
    public static LongIntHashMap read(IOHandler handler, ByteBuffer byteBuffer) throws IOException {
        int fileSize = (int) handler.length();
        int keyCount = fileSize / StoreConfig.VALUE_INDEX_RECORD_SIZE;
        int initSize = (int) ((keyCount+100)/StoreConfig.TROVE_LOAD_FACTOR);
        //logger.info("mapSize " +mapSize);
        LongIntHashMap map = new LongIntHashMap(initSize,StoreConfig.TROVE_LOAD_FACTOR);
        int bufferSize=byteBuffer.capacity();
        long key=0;
        long value=0;
        int remaining=0;
        byteBuffer.clear();
        handler.position(0);
        int oldValue;
        int segmentNo;
        int segmentOffset;
        long segmentSizeMask=StoreConfig.SEGMENT_LOG_FILE_SIZE-1;
        long rightShift= Bytes.bitSpace(StoreConfig.SEGMENT_LOG_FILE_SIZE);
        int  maxRecordInSingleLog=StoreConfig.SEGMENT_LOG_FILE_SIZE/StoreConfig.VALUE_SIZE;
        int leftShift=Bytes.bitSpace(maxRecordInSingleLog);
        int intValue;
        do {
            handler.read(byteBuffer);
            byteBuffer.flip();
            remaining=byteBuffer.remaining();
            while (byteBuffer.remaining() >= StoreConfig.VALUE_INDEX_RECORD_SIZE) {
                key = byteBuffer.getLong();
                value = byteBuffer.getLong();
                segmentNo=(int)(value>>>rightShift);
                segmentOffset=(int)(value&segmentSizeMask)/StoreConfig.LOG_ELEMENT_SIZE;
                intValue=(segmentNo<<leftShift)+segmentOffset;
                //logger.info(String.format("segment %d,segment offset %d,int value %d",segmentNo,segmentOffset,intValue));
                oldValue=map.put(key, intValue);
                // value 版本号
                if(oldValue>intValue){
                    // 保留大版本号
                    map.put(key,oldValue);
                    if(duplicatedCounter.incrementAndGet()%1000==0)
                        logger.info(String.format("duplicate key %d,newer version %d,old version %d",key,oldValue,value));
                }
                keyCounter.incrementAndGet();
            }
            byteBuffer.compact();
        }while (remaining==bufferSize);
        if(readBucketCounter.incrementAndGet()<=100) {
            logger.info(String.format("file %s contain %d index,init map %d,total key %d,this key %d,v %d ", handler.name(), handler.length() / StoreConfig.VALUE_INDEX_RECORD_SIZE,initSize, keyCounter.get(), key, value));
        }
           return map;
    }

    /**
     *
     * 限制初始加载缓存数量 handler 限制
     * @param  concurrency if handler 数量大于并行度，则每个任务会分配多个index 加载index cache 任务
     * @param service  执行加载cache 任务的 线程池
     * @param handlers 所有待加载cache 的 index 文件句柄
     * @param  cacheListener cache 加载任务完成回调
     *
     **/
    public void concurrentLoadIndex(ExecutorService service,int concurrency,List<ByteBuffer> buffers, List<IOHandler> handlers , CacheListener cacheListener) throws Exception{
        if(!Null.isEmpty(handlers)){
            if(concurrency>buffers.size()) throw new IllegalArgumentException("need more buffer bucket");
            int perThreadTasks=1;
            int mod=0;
            int handlerCount=handlers.size();
            if(handlerCount>concurrency){
                perThreadTasks=handlerCount/concurrency;
                mod=handlerCount%concurrency;
            }else{
                concurrency=handlerCount;
            }
            if(service==null){
                service= Executors.newFixedThreadPool(concurrency);
            }
            int start=0;
            int end;
            for(int i=0;i<concurrency;i++){
                end=start;
                if(i<mod){
                    end+=1;
                }
                end+=perThreadTasks;
                logger.info(String.format("assign task start %d ,end  %d",start,end));
                service.submit(new LoadIndexTask(handlers,start,end,buffers.get(i),cacheListener));
                start=end;
            }
//            service.shutdown();
//            service.awaitTermination(StoreConfig.LOAD_HASH_INDEX_TIMEOUT, TimeUnit.MILLISECONDS);
        }


    }

    public class LoadIndexTask implements Runnable{
        private List<IOHandler> handlers;
        private ByteBuffer buffer;
        private CacheListener cacheListener;
        private int start;
        private int end;
        private IOHandler handler;
        public LoadIndexTask(List<IOHandler> handlers,int start,int end,ByteBuffer buffer,CacheListener cacheListener){
            this.handlers=handlers;
            this.buffer=buffer;
            this.cacheListener=cacheListener;
            this.start=start;
            this.end=end;

        }
        @Override
        public void run() {
            try {
                for(int i=start;i<end;i++) {
                    handler=handlers.get(i);
                    LongIntHashMap map = IndexReader.read(handler, buffer);
                    if(cacheListener!=null)
                        cacheListener.onCache(Integer.valueOf(handler.name()),map);
                    onIndexClose();
                }
            }catch (IOException e){
                logger.info(String.format("load key failed,%d ",handler.name()));
            }

        }
        /**
         * close 所有的io handler,hash bucket file always open
         **/
        public void onIndexClose() throws IOException{
                handler.closeFileChannel(false);
                // whole file
                logger.info(String.format("load key success and close,%d ",handler.name()));
                handler.dontNeed(0,0);
        }
    }
}
