package com.alibabacloud.polar_race.engine.kv.index;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.io.FileChannelIOHandler;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.common.utils.KeyValueArray;
import com.alibabacloud.polar_race.engine.kv.buffer.BufferHolder;
import com.alibabacloud.polar_race.engine.kv.buffer.LogBufferAllocator;
import com.alibabacloud.polar_race.engine.kv.cache.IOHandlerLRUCache;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.wal.IndexServiceManager;
import org.rocksdb.WBWIRocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import com.alibabacloud.polar_race.engine.kv.partition.Range;
/**
 *  range kv index
 *
 **/
public class SequentialIndexService extends Service implements IndexService {
    private final static Logger logger= LoggerFactory.getLogger(SequentialIndexService.class);
    private LogFileService partitionWalLogFIleService;
    private IOHandlerLRUCache partitionIOHandlerCache;
    private int keyCapacity=0;
    private int keySize=0;
    private long segmentSizeMask=StoreConfig.SEGMENT_LOG_FILE_SIZE-1;
    private long rightShift= Bytes.bitSpace(StoreConfig.SEGMENT_LOG_FILE_SIZE);
    private int  maxRecordInSingleLog=StoreConfig.SEGMENT_LOG_FILE_SIZE/StoreConfig.VALUE_SIZE;
    private int  maxRecordMask=maxRecordInSingleLog-1;
    private int leftShift=Bytes.bitSpace(maxRecordInSingleLog);
    private IndexWalLogReader indexWalLogReader;
    private KeyValueArray indexArray;
    private String orderLogSubDir=StoreConfig.ORDERED_LOG_CHILD_DIR;
    private Range partition;
    private IndexServiceManager indexServiceManager;
    private List<IOHandler> batchTransferedHandler=new ArrayList<>(100);
    private BlockingQueue<ByteBuffer> readBuffers=new ArrayBlockingQueue(10);
    private BlockingQueue<ByteBuffer> bufferPool=new ArrayBlockingQueue(10);
     private ExecutorService executorService;
    private int batchReadSize=16;
    int perOrderLogFileMaxSize =StoreConfig.SEGMENT_LOG_FILE_SIZE-StoreConfig.SEGMENT_LOG_FILE_SIZE%StoreConfig.LOG_ELEMENT_SIZE;
    public SequentialIndexService(LogFileService partitionWalLogFileService, IOHandlerLRUCache partitionIOHandlerCache, Range partition, IndexServiceManager indexServiceManager,ExecutorService comm){
        this.partitionWalLogFIleService=partitionWalLogFileService;
        this.partitionIOHandlerCache=partitionIOHandlerCache;
        this.partition=partition;
        this.indexServiceManager=indexServiceManager;
        //this.executorService=Executors.newFixedThreadPool(batchReadSize);
        Files.makeDirIfNotExist(partitionWalLogFIleService.dir()+orderLogSubDir);
        Files.emptyDirIfExist(partitionWalLogFIleService.dir()+orderLogSubDir);
    }

    @Override
    public void onStart() throws Exception {
        //super.onStart();
        loadIndex();
        // sequential put
        // consider disk capacity
        //indexServiceManager.get(partition.getPartitionId()); // may blocking until disk enough
        partitionIOHandlerCache.prepareToTransfer(); // init transfer stat
        executorService=Executors.newFixedThreadPool(batchReadSize);
        int poolSize=10;
        for(int i=0;i<poolSize;i++)
            bufferPool.put(ByteBuffer.allocateDirect(StoreConfig.SEGMENT_LOG_FILE_SIZE));
//         WriteThread    wr=new WriteThread(0);
//         new Thread(wr).start();
         orderWalog();
         //wr.stop();
         partition.setPartition(indexArray);
         partition.setIndexService(this);
         for(int i=0;i<poolSize;i++) {
            ByteBuffer buffer= bufferPool.poll(10,TimeUnit.MILLISECONDS);
            if(buffer!=null)
                LogBufferAllocator.release(buffer);
         }
         //executorService.shutdownNow();
    }

    /**
     * 加载索引到内存
     *
     **/
    public void loadIndex(){
         long start=System.currentTimeMillis();
         List<Long>  logFiles=partitionWalLogFIleService.allLogFiles();
         // read last file and caculate  init parameters
         int keySize=maxKeySize();
         this.indexArray=new KeyValueArray(keySize);
         logger.info("index size "+indexArray.getSize());
         this.indexWalLogReader=new IndexWalLogReader(partitionWalLogFIleService, partitionIOHandlerCache, logFiles, 0, logFiles.size() - 1, new IndexKeyVisitor());
         this.indexWalLogReader.run();
         // sort key array, ready to transfer put to order
         //sortHelper.quickSort(keys,null,0,keySize-1);
         //indexArray.quickSort(indexArray.getKeys(),indexArray.getValues(),0,indexArray.getSize()-1);
         indexArray.compact();
         logger.info(String.format("partition %d load index finish,predict key size %d," +
                 "key size %d, elapse %d ms",partition.getPartitionId(),keySize,indexArray.getSize(),System.currentTimeMillis()-start));
         // 遍历去重
         //ascendingIncrease(indexArray.getKeys(),indexArray.getValues(),indexArray.getSize()-1);
         // order put file
    }

    public int maxKeySize(){
        long size=Long.valueOf(partitionWalLogFIleService.lastLogName());
        return (int)(size/StoreConfig.LOG_ELEMENT_SIZE);
    }

    /***
     *  根据key 的顺序
     *  整理wal put 为有序记录
     *
     * */
    public void orderWalog() throws EngineException {
         long start=System.currentTimeMillis();
         long[] keys=indexArray.getKeys();
         int[]  values=indexArray.getValues();
         int size=indexArray.getSize();
         int  offset;
         long key;
        long orderLongFileSize=0;
        int fileWriteCount=0;
        FileChannelIOHandler originIOHandler;
        FileChannelIOHandler destIOHandler;
        Random random=new Random();
        try {
            //destIOHandler=(FileChannelIOHandler) partitionWalLogFIleService.ioHandler(orderLogSubDir+orderLongFileSize+StoreConfig.LOG_FILE_SUFFIX);
            ByteBuffer readBuffer=bufferPool.take();
            for (int i = 0; i < size;) {
                int realBatchSize=Math.min(batchReadSize,size-i);
                CountDownLatch latch=new CountDownLatch(realBatchSize);
                for (int k=0; k <realBatchSize ; k++) {
                    //key = keys[k];
                    offset = values[i+k];
                    executorService.submit(new RandomRead(latch,readBuffer,offset,k));
                }
                i+=realBatchSize;
                        if(latch.getCount()==0||latch.await(5,TimeUnit.MILLISECONDS)) {
                            readBuffer.position(realBatchSize * StoreConfig.LOG_ELEMENT_SIZE);
                            readBuffer.flip();
                            readBuffer.clear(); // prepare to read
                            fileWriteCount += batchReadSize * StoreConfig.LOG_ELEMENT_SIZE;
                        }
            }
        }catch (Exception e){
            throw new EngineException(RetCodeEnum.IO_ERROR, e.getMessage());
        }
        logger.info(String.format("partition %d transfer to ordered log finish, elapse %d",partition.getPartitionId(),System.currentTimeMillis()-start));
    }

    public class RandomRead implements Runnable {
        private int offset;
        private int batchRead;
        private IOHandler handler;
        private ByteBuffer readBuffer;
        private CountDownLatch latch;
        public RandomRead(CountDownLatch latch, ByteBuffer buffer, int offset,int batchRead){
            this.offset=offset;
            this.batchRead=batchRead;
            this.readBuffer=buffer;
            this.latch=latch;
        }

        @Override
        public void run() {
            try {
                int fileId = offset >>> leftShift;
               // logger.info("read "+fileId);
                int segmentOffset = offset & maxRecordMask;
                long offsetInFile = segmentOffset * StoreConfig.LOG_ELEMENT_SIZE;
                handler = partitionIOHandlerCache.getHandler(fileId);//logFileService.ioHandler(filename + StoreConfig.LOG_FILE_SUFFIX,"r");////logHandlerCache.getHandler(fileId);//
                if (handler == null) {
                    logger.info("                    logger.info(io handler not found);");
                    throw new EngineException(RetCodeEnum.IO_ERROR, "io handler not found");

                }
                // 随机读,顺序写
                readBuffer.position(batchRead * StoreConfig.VALUE_SIZE);
                readBuffer = readBuffer.slice();
                readBuffer.limit(StoreConfig.VALUE_SIZE);
                handler.read(offsetInFile+10, readBuffer);
                //handler.closeFileChannel(false);
            }catch (Exception e){
                logger.info("read error",e);
            }finally {
                latch.countDown();
            }
        }
    }

    public class WriteThread implements Runnable{

        long startOffset;
        long writeSize=0;
        private volatile boolean started=true;
        IOHandler handler;
        public WriteThread(long startOffset){
            this.startOffset=startOffset;
            this.writeSize=startOffset;
            try {
                this.handler = partitionWalLogFIleService.ioHandler(orderLogSubDir + writeSize + StoreConfig.LOG_FILE_SUFFIX);
            }catch (FileNotFoundException e){
                e.printStackTrace();
            }
        }
        public void stop(){
            this.started=false;
        }
        @Override
        public void run() {
            ByteBuffer readBuffer;
            try {
                while (true) {
                    readBuffer = readBuffers.poll(1000, TimeUnit.MILLISECONDS);
                    if (readBuffer == null) {
                        if(started) {
                    //        logger.info("no read buffer ,to write");
                            continue;
                        }else {
                            break;
                        }
                    }
                    writeSize+=readBuffer.remaining();
                    //handler.append(readBuffer);
                    bufferPool.put(readBuffer); //return
                    if(writeSize>=perOrderLogFileMaxSize){
                        handler.closeFileChannel(false);
                        handler=partitionWalLogFIleService.ioHandler(orderLogSubDir + writeSize + StoreConfig.LOG_FILE_SUFFIX);
                    }
                }
            }catch (Exception e){
                logger.info("write error",e);
            }
        }
    }

    @Override
    public int getOffset(long key) throws EngineException {
        return 0;
    }

    @Override
    public void range(long lower, long upper, AbstractVisitor iterator) {

    }

    /**
     * @param start indclude ,index in index array
     * @param end exclusive
     *
     **/
    public void range(int start, int end, AbstractVisitor visitor) throws EngineException{
        long globalStartOffset=start* StoreConfig.LOG_ELEMENT_SIZE;
        long globalEndOffset=end*StoreConfig.LOG_ELEMENT_SIZE;
        int offsetInFirstFile=(int)(globalStartOffset%perOrderLogFileMaxSize);
        int offsetInLastFile=(int)(globalEndOffset%perOrderLogFileMaxSize);
        int firstFileSequenceId=(int)(globalStartOffset/perOrderLogFileMaxSize);
        long lastFileSequenceId=(int)(globalEndOffset/perOrderLogFileMaxSize);
        ByteBuffer buffer=ByteBuffer.allocateDirect(perOrderLogFileMaxSize);

        ByteBuffer value=ByteBuffer.allocate(StoreConfig.VALUE_SIZE);
        byte[]  key=new byte[8];
        try {
            IOHandler ioHandler=orderIOHandler(firstFileSequenceId);
            if(firstFileSequenceId==lastFileSequenceId){
                // read size
                buffer.limit(offsetInFirstFile-offsetInFirstFile);
                ioHandler.read(offsetInFirstFile, buffer);
                start=invokeVisitor(buffer,key,value,start,visitor);
                return;
            }
            // not start of the file
            if (offsetInFirstFile != 0) {
                //buffer.limit(perOrderLogFileMaxSize-offsetInFirstFile);
                ioHandler.read(offsetInFirstFile, buffer);
                start=invokeVisitor(buffer,key,value,start,visitor);
            }
            int fileSequenceId=firstFileSequenceId+1;
            for (; fileSequenceId < lastFileSequenceId; fileSequenceId++) {
                buffer.clear();
                ioHandler=orderIOHandler(fileSequenceId);
                ioHandler.read(buffer); // expect read full
                start=invokeVisitor(buffer,key,value,start,visitor);
            }
            if(fileSequenceId==lastFileSequenceId&&offsetInLastFile!=0){
                ioHandler=orderIOHandler(lastFileSequenceId);
                buffer.clear();
                buffer.limit(offsetInLastFile);
                ioHandler.read(buffer);
                start=invokeVisitor(buffer,key,value,start,visitor);
            }
            if(start!=end){
                logger.info("may bug");
            }
        }catch (IOException e){
            logger.info("range error",e);
            throw new EngineException(RetCodeEnum.IO_ERROR,"in range error");
        }
    }

    /**
     *
     **/
    private IOHandler orderIOHandler(long fileId) throws FileNotFoundException {
        return partitionWalLogFIleService.ioHandler(orderLogSubDir+(fileId*perOrderLogFileMaxSize)+StoreConfig.LOG_FILE_SUFFIX);
    }


    /**
     * @param end exclusive ,index in index array
     *  to the end,include
     *
     **/
    public void rangeFromStart(int end, AbstractVisitor visitor) throws EngineException{
        int firstFileSequenceId=0;
        long globalEndOffset=end*StoreConfig.LOG_ELEMENT_SIZE;
        int offsetInLastFile=(int)(globalEndOffset%perOrderLogFileMaxSize);
        long lastFileSequenceId=globalEndOffset/perOrderLogFileMaxSize; // last file
        ByteBuffer buffer=ByteBuffer.allocateDirect(perOrderLogFileMaxSize);
        IOHandler ioHandler;
        ByteBuffer value=ByteBuffer.allocate(StoreConfig.VALUE_SIZE);
        byte[]  key=new byte[8];
        try {
            int start=0;
            int fileSequenceId=0;
            for (; fileSequenceId < lastFileSequenceId; fileSequenceId++) {
                buffer.clear();
                ioHandler=orderIOHandler(fileSequenceId);
                ioHandler.read(buffer); // expect read full
                start=invokeVisitor(buffer,key,value,start,visitor);
            }
            if(fileSequenceId==lastFileSequenceId&&offsetInLastFile!=0){
                ioHandler=orderIOHandler(lastFileSequenceId);
                buffer.clear();
                buffer.limit(offsetInLastFile);
                ioHandler.read(buffer);
                start=invokeVisitor(buffer,key,value,start,visitor);
            }
            if(start!=indexArray.getSize()){
                logger.info("may bug");
            }
        }catch (IOException e){
            logger.info("range error",e);
            throw new EngineException(RetCodeEnum.IO_ERROR,"in range error");
        }
    }

    /**
     * call back
     * @return  next visit index
     **/
    public int invokeVisitor(ByteBuffer buffer,byte[] key,ByteBuffer value,int start,AbstractVisitor visitor){
        buffer.flip();
        short len;
        long longKey;
        // always multiple of record
        while(buffer.remaining()>StoreConfig.LOG_ELEMENT_SIZE){
            len= buffer.getShort();
            longKey=buffer.getLong();
            if(longKey!=indexArray.getKey(start++)){
                logger.info("bug");
            }
            value.clear();
            value.put(buffer);
            Bytes.long2bytes(longKey,key,0);
            visitor.visit(key,value.array());
        }
        if(buffer.hasRemaining()) logger.info("has remaining ,bug");
        return start;
    }


    /**
     * load key index
     **/
    public class IndexKeyVisitor implements IndexVisitor{
        private long key;
        private long offset;
        private int  intOffset;
        private int segmentNo;
        private int segmentOffset;
        @Override
        public void visit(ByteBuffer buffer) throws Exception {
            // contain n index record
            while(buffer.remaining()>= StoreConfig.VALUE_INDEX_RECORD_SIZE) {
//                if(keySize>=keyCapacity){
//                    // to enlarge
//                    logger.info("out of key array capacity");
//                    throw new IllegalStateException("out of key array capacity");
//                }
                key=buffer.getLong();
                offset= buffer.getLong();
                segmentNo=(int)(offset>>>rightShift);
                segmentOffset=(int)(offset&segmentSizeMask)/StoreConfig.LOG_ELEMENT_SIZE;
                intOffset=(segmentNo<<leftShift)+segmentOffset;
                // skip offset
                indexArray.put(key,intOffset);
            }
        }

        @Override
        public void onFinish() throws Exception {

        }

        @Override
        public void onException() {

        }
    }




}
