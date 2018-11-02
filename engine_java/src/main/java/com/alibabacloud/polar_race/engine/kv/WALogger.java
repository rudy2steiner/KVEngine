package com.alibabacloud.polar_race.engine.kv;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.kv.event.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class WALogger implements WALog<Put> {

    private final static Logger logger= LoggerFactory.getLogger(WALogger.class);
    private ThreadLocal<ByteBuffer> keyBuf=new ThreadLocal<>();
    private ThreadLocal<ByteBuffer> valueBuf=new ThreadLocal<>();
    private String dir;
    private ConcurrentHashMap<Long,ValueIndex> valueIndexMap;
    private LogFileService fileService;
    private MultiTypeLogAppender appender;
    public WALogger(String dir){
        this.dir=dir;
        Files.makeDirIfNotExist(dir);
        this.fileService=new LogFileServiceImpl(dir);

    }
    /**
     *  查看是否异常退出，并恢复日志完整性
     *
     **/
    public IOHandler replayLastLog(){

        return null;
    }

    /**
     * 将索引全部加载到内存
     **/
    public void loadKey(){
        valueIndexMap =new ConcurrentHashMap<>(StoreConfig.KEY_INDEX_MAP_INIT_CAPACITY);
    }

    @Override
    public long log(Put event) throws Exception{
           appender.append(event);
           ValueIndex index=new ValueIndex(event.value().getKey(),event.value().getOffset());
           valueIndexMap.put(Bytes.bytes2long(event.value().getKey(),0),index);
           return event.txId();
    }

    @Override
    public void iterate(AbstractVisitor visitor) throws IOException {
        List<Long> logNames=fileService.allLogFiles();
        LogParser parser;
        ByteBuffer to= ByteBuffer.allocate(StoreConfig.FILE_READ_BUFFER_SIZE);
        ByteBuffer from= ByteBuffer.allocate(StoreConfig.FILE_READ_BUFFER_SIZE);
        for(Long logName:logNames){
            parser=new LogParser(dir,logName+StoreConfig.LOG_FILE_SUFFIX);
            parser.parse(visitor,to,from);
            to.clear();
            from.clear();
        }
    }


    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) {

    }

    @Override
    public byte[] get(byte[] key) throws Exception{
            ValueIndex index= valueIndexMap.get(Bytes.bytes2long(key,0));
            String fileName=fileService.fileName(index.getOffset());
            //logger.info(String.format("offset %d find file name %s",index.getOffset(),fileName));
            if(fileName==null) return new byte[0];
            IOHandler handler=fileService.ioHandler(fileName);
            ByteBuffer keyBuffer=keyBuf.get();
                if(keyBuffer==null) {
                    keyBuffer = ByteBuffer.allocate(8);
                    keyBuf.set(keyBuffer);
                }
                keyBuffer.clear();
            ByteBuffer valueBuffer=valueBuf.get();
                if(valueBuffer==null){
                    valueBuffer=ByteBuffer.allocate(4096);
                    valueBuf.set(valueBuffer);
                }
                valueBuffer.clear();
            long offset=index.getOffset()-Long.valueOf(handler.name())+2;
            handler.position(offset);
            //logger.info(String.format("%s offset %d",fileName,offset));
            //logger.info(String.format("read key:%s offset %d",fileName,handler.position()));
            handler.read(keyBuffer);
            //logger.info(String.format("read value %s offset %d",fileName,handler.position()));
            handler.read(valueBuffer);
        return valueBuffer.array();
    }

    @Override
    public void start() throws Exception{
        IOHandler handler;
        String nextLogName;
        if(fileService.needReplayLog()){
            handler=replayLastLog();
            nextLogName=fileService.nextLogName(handler);
            handler=fileService.bufferedIOHandler(nextLogName,handler);
        }else{
            nextLogName=fileService.nextLogName();
            handler=fileService.bufferedIOHandler(nextLogName,StoreConfig.FILE_WRITE_BUFFER_SIZE);
        }
        loadKey();
       // String fileService.lastLogName();
        this.appender=new MultiTypeLogAppender(handler,fileService,StoreConfig.DISRUPTOR_BUFFER_SIZE);
        this.appender.start();
    }

    @Override
    public void close() throws Exception {
         this.appender.close();
    }
}
