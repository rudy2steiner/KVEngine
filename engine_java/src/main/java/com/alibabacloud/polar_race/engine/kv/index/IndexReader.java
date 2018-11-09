package com.alibabacloud.polar_race.engine.kv.index;
import com.alibabacloud.polar_race.collection.LongLongMap;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Null;
import com.alibabacloud.polar_race.engine.kv.cache.CacheListener;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
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
    public static LongLongMap read(IOHandler handler, ByteBuffer byteBuffer) throws IOException {
        int fileSize = (int) handler.length();
        int keyCount = fileSize / StoreConfig.VALUE_INDEX_RECORD_SIZE;
        int mapSize = (int) Math.ceil(keyCount * 1.23f);
        //logger.info("mapSize " +mapSize);
        LongLongMap map = LongLongMap.withExpectedSize(mapSize);
        int bufferSize=byteBuffer.capacity();
        long key=0;
        long value=0;
        int remaining=0;
        byteBuffer.clear();
        handler.position(0);
        long oldValue=0;
        do {
            handler.read(byteBuffer);
            byteBuffer.flip();
            remaining=byteBuffer.remaining();
            while (byteBuffer.remaining() >= StoreConfig.VALUE_INDEX_RECORD_SIZE) {
                key = byteBuffer.getLong();
                value = byteBuffer.getLong();
                oldValue=map.put(key, value);
                // value 版本号
                if(oldValue>value){
                    // 保留大版本号
                    map.put(key,oldValue);
                    if(duplicatedCounter.incrementAndGet()%1000==0)
                        logger.info(String.format("duplicate key %d,newer version %d,old version %d",key,oldValue,value));
                }
                keyCounter.incrementAndGet();
            }
            byteBuffer.compact();
        }while (remaining==bufferSize);
        if(readBucketCounter.incrementAndGet()<=100)
            logger.info(String.format("file %s ,total load key count %d,this key %d,v %d ",handler.name(),keyCounter.get(),key,value));
           return map;
    }

    /**
     *
     * 限制初始加载缓存数量 handler 限制
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
                    LongLongMap map = IndexReader.read(handler, buffer);
                    if(cacheListener!=null)
                        cacheListener.onCache(Integer.valueOf(handler.name()),map);
                }
                close();
            }catch (IOException e){
                logger.info(String.format("load key failed,%d ",handler.name()));
            }

        }
        /**
         *close 所有的io handler
         **/
        public void close(){
            for(int i=start;i<end;i++){
               indexFileService.asyncCloseFileChannel(handlers.get(i));
            }
        }
    }
}
