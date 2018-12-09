package com.alibabacloud.polar_race.engine.kv.index;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.Service;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.alibabacloud.polar_race.engine.common.io.FileChannelIOHandler;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.KeyValueArray;
import com.alibabacloud.polar_race.engine.kv.cache.IOHandlerLRUCache;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.wal.IndexServiceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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
    int perOrderLogFileMaxSize =StoreConfig.SEGMENT_LOG_FILE_SIZE-StoreConfig.SEGMENT_LOG_FILE_SIZE%StoreConfig.LOG_ELEMENT_SIZE;
    public SequentialIndexService(LogFileService partitionWalLogFIleService, IOHandlerLRUCache partitionIOHandlerCache, Range partition, IndexServiceManager indexServiceManager){
        this.partitionWalLogFIleService=partitionWalLogFIleService;
        this.partitionIOHandlerCache=partitionIOHandlerCache;
        this.partition=partition;
        this.indexServiceManager=indexServiceManager;
    }

    @Override
    public void onStart() throws Exception {
        //super.onStart();
        loadIndex();
        // sequential put
        // consider disk capacity
        indexServiceManager.get(partition.getPartitionId()); // may blocking until disk enough
        orderWalog();
        partition.setPartition(indexArray);
        partition.setIndexService(this);
    }

    /**
     * 加载索引到内存
     *
     **/
    public void loadIndex(){
         long start=System.currentTimeMillis();
         List<Long>  logFiles=partitionWalLogFIleService.allLogFiles();
         // read last file and caculate  init parameters
         this.indexArray=new KeyValueArray(1000);
         this.indexWalLogReader=new IndexWalLogReader(partitionWalLogFIleService, partitionIOHandlerCache, logFiles, 0, logFiles.size() - 1, new IndexKeyVisitor());
         this.indexWalLogReader.run();
         // sort key array, ready to transfer put to order
         //sortHelper.quickSort(keys,null,0,keySize-1);
         indexArray.quickSort(indexArray.getKeys(),indexArray.getValues(),0,indexArray.getSize()-1);
         indexArray.compact();
         logger.info(String.format("partition %d load index finish, elapse %d",partition.getPartitionId(),System.currentTimeMillis()-start));
         // 遍历去重
         //ascendingIncrease(indexArray.getKeys(),indexArray.getValues(),indexArray.getSize()-1);
         // order put file
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
        try {
            destIOHandler=(FileChannelIOHandler) partitionWalLogFIleService.ioHandler(orderLogSubDir+orderLongFileSize+StoreConfig.LOG_FILE_SUFFIX);
            for (int i = 0; i < size; i++) {
                key = keys[i];
                offset = values[i];
                if (offset >=0) {
                    int fileId = offset >>> leftShift;
                    int segmentOffset = offset & maxRecordMask;
                    long offsetInFile = segmentOffset * StoreConfig.LOG_ELEMENT_SIZE;
                    originIOHandler = (FileChannelIOHandler) partitionIOHandlerCache.getHandler(fileId);//logFileService.ioHandler(filename + StoreConfig.LOG_FILE_SUFFIX,"r");////logHandlerCache.getHandler(fileId);//
                    if (originIOHandler == null)
                        throw new EngineException(RetCodeEnum.IO_ERROR, "io handler not found");
                    // 随机读,顺序写
                    originIOHandler.getFileChannel().transferTo(offsetInFile, StoreConfig.LOG_ELEMENT_SIZE, destIOHandler.getFileChannel());
                    fileWriteCount+=StoreConfig.LOG_ELEMENT_SIZE;
                    if(fileWriteCount== perOrderLogFileMaxSize){
                        // roll put file
                        orderLongFileSize+= perOrderLogFileMaxSize;
                        // may need flush
                        destIOHandler=(FileChannelIOHandler) partitionWalLogFIleService.ioHandler(orderLogSubDir+orderLongFileSize+StoreConfig.LOG_FILE_SUFFIX);
                        fileWriteCount=0;
                    }
                    // transfer record action
                    if(partitionIOHandlerCache.transfer(fileId,(int)offsetInFile)){
                        // consider notify delete old file and to
                        batchTransferedHandler.add(originIOHandler);
                        if(batchTransferedHandler.size()==StoreConfig.BATCH_IOHANDLER){
                            indexServiceManager.release(batchTransferedHandler);
                            batchTransferedHandler.clear();
                        }
                    }
                } else {
                    logger.info(String.format("ignore duplicate %d,try to keep newest", key));
                }
            }
        }catch (IOException e){
            throw new EngineException(RetCodeEnum.IO_ERROR, e.getMessage());
        }
        logger.info(String.format("partition %d transfer to ordered log finish, elapse %d",partition.getPartitionId(),System.currentTimeMillis()-start));
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
        IOHandler ioHandler=partitionIOHandlerCache.getHandler(firstFileSequenceId);
        ByteBuffer value=ByteBuffer.allocate(StoreConfig.VALUE_SIZE);
        byte[]  key=new byte[8];
        try {
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
                ioHandler=partitionIOHandlerCache.getHandler(fileSequenceId);
                ioHandler.read(buffer); // expect read full
                start=invokeVisitor(buffer,key,value,start,visitor);
            }
            if(fileSequenceId==lastFileSequenceId&&offsetInLastFile!=0){
                ioHandler=partitionIOHandlerCache.get(lastFileSequenceId);
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
                ioHandler=partitionIOHandlerCache.getHandler(fileSequenceId);
                ioHandler.read(buffer); // expect read full
                start=invokeVisitor(buffer,key,value,start,visitor);
            }
            if(fileSequenceId==lastFileSequenceId&&offsetInLastFile!=0){
                ioHandler=partitionIOHandlerCache.get(lastFileSequenceId);
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
                if(keySize>=keyCapacity){
                    // to enlarge
                    logger.info("out of key array capacity");
                    throw new IllegalStateException("out of key array capacity");
                }
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
