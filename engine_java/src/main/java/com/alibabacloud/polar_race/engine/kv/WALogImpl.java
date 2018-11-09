package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.kv.event.Cell;
import com.alibabacloud.polar_race.engine.kv.event.EventBus;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.file.LogFileServiceImpl;
import com.alibabacloud.polar_race.engine.kv.wal.WALog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class WALogImpl implements WALog<Cell> {
    private String dir;
    private  LogAppender logAppender;
    private LogFileService fileService;
    private EventBus ioHandlerCloseProcessor;
    public WALogImpl(String dir, EventBus eventBus) throws FileNotFoundException {
        this.dir=dir;
        makeDirIfNotExist(dir);
        this.ioHandlerCloseProcessor=eventBus;
        if(!isEmptyDir(dir)){
            //throw new IllegalArgumentException("not empty dir");
        }
        this.fileService=new LogFileServiceImpl(dir,ioHandlerCloseProcessor);
        this.logAppender=new LogAppender(fileService.bufferedIOHandler("0"+ StoreConfig.LOG_FILE_SUFFIX,StoreConfig.FILE_WRITE_BUFFER_SIZE),
                                         fileService,StoreConfig.DISRUPTOR_BUFFER_SIZE);
    }

    @Override
    public long log(Cell cell) {
        this.logAppender.append(cell);
        return cell.getTxId();
    }

    @Override
    public void iterate(AbstractVisitor visitor) throws IOException {
        List<Long> logNames=fileService.allLogFiles();
        LogParser parser;
        ByteBuffer to= ByteBuffer.allocate(StoreConfig.FILE_READ_BUFFER_SIZE);
        ByteBuffer from= ByteBuffer.allocate(StoreConfig.FILE_READ_BUFFER_SIZE);
        for(Long logName:logNames){
             parser=new LogParser(dir,String.valueOf(logName));
             parser.parse(visitor,to,from);
             to.clear();
             from.clear();
        }

    }

    @Override
    public void start() {
        this.logAppender.start();
    }

    @Override
    public void close() throws Exception{
        this.logAppender.close();
    }

    public void  makeDirIfNotExist(String dir){
            File file=new File(dir);
            if(file.exists()) return;
            file.mkdirs();
    }
    public boolean isEmptyDir(String dir) {
        File file=new File(dir);
        if(file.exists()){
           String[] files= file.list();
           if(files.length>0) return false;
        }
        return true;
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) {

    }

    @Override
    public byte[] get(byte[] key) {
        return new byte[0];
    }
}
