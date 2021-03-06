package com.alibabacloud.polar_race.engine.kv.file;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.*;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Memory;
import com.alibabacloud.polar_race.engine.common.utils.Null;
import com.alibabacloud.polar_race.engine.kv.event.Cell;
import com.alibabacloud.polar_race.engine.kv.event.TaskBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LogFileServiceImpl implements LogFileService{
    private final static Logger logger= LoggerFactory.getLogger(LogFileServiceImpl.class);
    private String dir;
    private List<Long> sortedLogFiles;
    private int  logWritableSize;
    private int  logTailerAndIndexSize;
    private CloseHandler handlerCloseEventProcessor;
    private volatile long fileTotalSpace;
    private long lastWriteFile;
    public final static AtomicInteger closeHandlerCounter=new AtomicInteger(0);
    public LogFileServiceImpl(String dir, CloseHandler eventBus){
        this.dir=dir;
        this.handlerCloseEventProcessor=eventBus;
        scanFiles();
    }

    public void scanFiles(){
        fileTotalSpace=0;
        this.sortedLogFiles =allLogFiles();
    }
    @Override
    public String nextLogName(Cell cell) {
        return Bytes.bytes2long(cell.getKey(),0)+ StoreConfig.LOG_FILE_SUFFIX;
    }

    @Override
    public String nextLogName(IOHandler handler) {
        lastWriteFile=Long.valueOf(handler.name())+StoreConfig.SEGMENT_LOG_FILE_SIZE;
        return String.valueOf(lastWriteFile)+StoreConfig.LOG_FILE_SUFFIX;
    }

    @Override
    public String nextLogName() {
        String lastLogName=lastLogName();
        return String.valueOf(Long.valueOf(lastLogName==null?String.valueOf(-StoreConfig.SEGMENT_LOG_FILE_SIZE):lastLogName)+StoreConfig.SEGMENT_LOG_FILE_SIZE)+StoreConfig.LOG_FILE_SUFFIX;
    }

    @Override
    public IOHandler bufferedIOHandler(String fileName, int bufferSize) throws FileNotFoundException {

        return bufferedIOHandler(fileName,bufferSize,"rw");
    }

    @Override
    public IOHandler bufferedIOHandler(String fileName, int bufferSize, String mode) throws FileNotFoundException {
        File file=new File(dir,fileName);
        IOHandler handler=new FileChannelIOHandler(file,mode);
        return new BufferedIOHandler(handler,bufferSize);
    }

    @Override
    public IOHandler bufferedIOHandler(String fileName, IOHandler handler, String mode) throws FileNotFoundException {
        File file=new File(dir,fileName);
        IOHandler newHandler=new FileChannelIOHandler(file,mode);
        //asyncCloseFileChannel(handler);
        asyncCloseFileChannel(handler);
        return new BufferedIOHandler(newHandler,handler.buffer());
    }

    @Override
    public IOHandler bufferedIOHandler(String fileName,IOHandler handler) throws FileNotFoundException {
        return  bufferedIOHandler(fileName,handler,"rw");
    }

    @Override
    public IOHandler ioHandler(String fileName) throws FileNotFoundException {
        return new FileChannelIOHandler(new File(dir,fileName),"rw");
    }


    @Override
    public IOHandler ioHandler(String fileName, String mode) throws FileNotFoundException {
        return new FileChannelIOHandler(new File(dir,fileName),mode);
    }

    @Override
    public File nextLogFile(Cell cell) {
        return new File(dir,nextLogName(cell));
    }

    @Override
    public List<Long> allLogFiles() {
        return  allSortedFiles(StoreConfig.LOG_FILE_SUFFIX);
    }

    @Override
    public List<Long> allSortedFiles(String suffix) {
        fileTotalSpace=0;
        File file=new File(dir);
        List<Long> logNames=new ArrayList<>();
        if(!file.isDirectory()) return null;
        String[] names= file.list() ;
        for(String name:names){
            if(name.endsWith(suffix)){
                logNames.add(Long.valueOf(name.substring(0,name.indexOf('.'))));
                addByteSize(new File(dir,name).length());
            }
        }
        Collections.sort(logNames);
        return logNames;
    }

    public long addByteSize(long size){
        fileTotalSpace+=size;
        return fileTotalSpace;
    }

    @Override
    public String lastLogName() {
        if(!Null.isEmpty(sortedLogFiles))
           return String.valueOf(sortedLogFiles.get(sortedLogFiles.size()-1));
        return null;
    }

    @Override
    public int tailerAndIndexSize() {
        if(logTailerAndIndexSize<=0) {
            int size = (int)(StoreConfig.SEGMENT_LOG_FILE_SIZE / StoreConfig.VALUE_SIZE) * StoreConfig.VALUE_INDEX_RECORD_SIZE;
            if (size % StoreConfig.K4_SIZE == 0) {
                logTailerAndIndexSize = size;
            } else {
                logTailerAndIndexSize=size-size % StoreConfig.K4_SIZE+StoreConfig.K4_SIZE;
            }
        }
        return logTailerAndIndexSize;
    }

    @Override
    public int logWritableSize() {
        if(logWritableSize<=0) {
           logWritableSize=StoreConfig.SEGMENT_LOG_FILE_SIZE- tailerAndIndexSize();
        }
        return logWritableSize;
    }

    @Override
    public boolean needReplayLog() {
        String lastName=lastLogName();
        if(lastName!=null) {
            File file = new File(dir,lastName+StoreConfig.LOG_FILE_SUFFIX );
            if (file.exists()){
                long fileSize=file.length();
                if(fileSize>0l&&fileSize<StoreConfig.SEGMENT_LOG_FILE_SIZE){
                    return true;
                }else if(fileSize==0){
                     logger.info("delete last empty file "+lastName);
                     file.delete();
                     sortedLogFiles.remove(sortedLogFiles.size()-1);
                }else{
                    logger.info("last time shutdown gracefully,last log "+lastName);
                }
            }
        }
        return false;
    }

    /**
     * binary search
     * @return  position 所在的文件名,带后缀
     **/
    @Override
    public String fileName(long position) {
        int mid;
        long value=position;
        long midValue=-1;
        if(Null.isEmpty(sortedLogFiles)) scanFiles();
        if (!Null.isEmpty(sortedLogFiles)) {
            int low = 0;
            int high = sortedLogFiles.size() - 1;
            while (low < high) {
                mid = low + (high - low) / 2;
                midValue = Long.valueOf(sortedLogFiles.get(mid));
                if (midValue < value) {
                    if (Long.valueOf(sortedLogFiles.get(mid + 1)) > value)
                        break;
                    low = mid + 1;
                } else if (midValue > value) {
                    high = mid - 1;
                } else break;

            }
            if (low == high) midValue = Long.valueOf(sortedLogFiles.get(high));
            return String.valueOf(midValue)+StoreConfig.LOG_FILE_SUFFIX;
        }
        return null;
    }

    @Override
    public void asyncCloseFileChannel(IOHandler handler) {
        handlerCloseEventProcessor.close(handler);
    }


    /**
     * async close the handler
     **/
//   public class CloseFileTask implements Runnable {
//
//    }

    @Override
    public int mapInitSize(int expectSize, float loadFactor)
    {
            return  (int)(( expectSize+100)/loadFactor);
    }

    @Override
    public File file(long fileName,String suffix) {
        return  new File(dir,fileName+suffix);
    }

    @Override
    public long lastWriteLogName(boolean realTime) {
        return lastWriteFile;
    }

    @Override
    public int expectedSize() {
        return (int)(StoreConfig.MAX_LOG_SPACE_SIZE/StoreConfig.SEGMENT_LOG_FILE_SIZE);
    }

    @Override
    public int size() {
        File file=new File(dir);
        if(!file.isDirectory()) return 0;
        return  file.list().length ;
    }

    @Override
    public int maxRecordInSingleFile() {
        return StoreConfig.SEGMENT_LOG_FILE_SIZE/StoreConfig.VALUE_SIZE;
    }
}
