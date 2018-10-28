package com.alibabacloud.polar_race.engine.kv;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.kv.event.Put;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class WALogger implements WALog<Put> {

    private String dir;
    private ConcurrentHashMap<byte[],ValueIndex> valueIndexMap;
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
           valueIndexMap.put(event.value().getKey(),new ValueIndex(event.value().getKey(),event.value().getOffset()));
           return event.txId();
    }

    @Override
    public void iterate(AbstractVisitor visitor) throws IOException {

    }


    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) {

    }

    @Override
    public byte[] get(byte[] key) throws Exception{
            ValueIndex index= valueIndexMap.get(key);
            String fileName=fileService.fileName(index.getOffset());
            if(fileName==null) return new byte[0];
            IOHandler handler=fileService.ioHandler(fileName);
            ByteBuffer keyBuf=ByteBuffer.allocate(8);
            ByteBuffer valueBuf=ByteBuffer.allocate(4096);
            handler.position(index.getOffset()-Long.valueOf(handler.name())+2);
            handler.read(keyBuf);
            handler.read(valueBuf);
        return valueBuf.array();
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
       // String fileService.lastLogName();
        this.appender=new MultiTypeLogAppender(handler,fileService,StoreConfig.DISRUPTOR_BUFFER_SIZE);
    }

    @Override
    public void close() throws Exception {

    }
}
