package com.alibabacloud.polar_race.engine.kv.file;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.BufferedIOHandler;
import com.alibabacloud.polar_race.engine.common.io.FileChannelIOHandler;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Memory;
import com.alibabacloud.polar_race.engine.common.utils.MemoryInfo;
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
    private  final static StringBuilder memoryInfoB=new StringBuilder();
    private String dir;
    private List<Long> sortedLogFiles;
    private int  logWritableSize;
    private int  logTailerAndIndexSize;
    private TaskBus handlerCloseEventProcessor;
    final static AtomicInteger closeHandlerCounter=new AtomicInteger(0);
    public LogFileServiceImpl(String dir, TaskBus eventBus){
        this.dir=dir;
        this.handlerCloseEventProcessor=eventBus;
        scanFiles();
    }

    public void scanFiles(){
        this.sortedLogFiles =allLogFiles();
    }
    @Override
    public String nextLogName(Cell cell) {
        return Bytes.bytes2long(cell.getKey(),0)+ StoreConfig.LOG_FILE_SUFFIX;
    }

    @Override
    public String nextLogName(IOHandler handler) {
        return String.valueOf(Long.valueOf(handler.name())+StoreConfig.SEGMENT_LOG_FILE_SIZE)+StoreConfig.LOG_FILE_SUFFIX;
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
        File file=new File(dir);
        List<Long> logNames=new ArrayList<>();
        if(!file.isDirectory()) return null;
        String[] names= file.list() ;
        for(String name:names){
            if(name.endsWith(suffix)){
                logNames.add(Long.valueOf(name.substring(0,name.indexOf('.'))));
            }
        }
        Collections.sort(logNames);
        return logNames;
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
            int size = (int)(StoreConfig.SEGMENT_LOG_FILE_SIZE / StoreConfig.VALUE_SIZE) * StoreConfig.KEY_SIZE;
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
                    //
                    logger.info("last time shutdown gracefully,"+lastName);

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
        handlerCloseEventProcessor.submit(new CloseFileTask(handler));
    }


    /**
     * async close the handler
     **/
   public class CloseFileTask implements Runnable {
        private IOHandler handler;

        public CloseFileTask(IOHandler handler){
            this.handler=handler;
        }
        @Override
        public void run() {
            try {
                long start=System.currentTimeMillis();
                int closed=closeHandlerCounter.incrementAndGet();
                if(closed%10000==0){
                    memoryInfoB.setLength(0);
                    logger.info(String.format("closed %d io handler,and close this %s now,time %d ms",closed,handler.name(),System.currentTimeMillis()-start));
                    MemoryInfo memoryInfo = Memory.memory();
                    memoryInfoB.append(memoryInfo.toString()).append("\n");
                    handler.closeFileChannel();
                    memoryInfoB.append(Memory.memory().toString());
//                    if(closed%10000==0&&handler instanceof BufferedIOHandler) {
//                        if (memoryInfo.getBufferCache() > StoreConfig.PAGE_CACHE_LIMIT){
//                            Memory.sync();
//                        }
//                        memoryInfoB.append(Memory.memory().toString());
//                    }
                   logger.info(memoryInfoB.toString());
                }else   handler.closeFileChannel();
            }catch (IOException e){
                logger.info(String.format("asyncClose %s exception,ignore",handler.name()),e);
            }
        }
    }

    @Override
    public int mapInitSize(int expectSize, float loadFactor)
    {
            return  (int)(( expectSize+100)/loadFactor);
    }

    @Override
    public File file(long fileName,String suffix) {
        return  new File(dir,fileName+suffix);
    }
}
