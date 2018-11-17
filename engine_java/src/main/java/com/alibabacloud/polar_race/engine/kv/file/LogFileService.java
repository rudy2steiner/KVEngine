package com.alibabacloud.polar_race.engine.kv.file;

import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.kv.event.Cell;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

public interface LogFileService {
    String nextLogName(Cell cell);
    String nextLogName(IOHandler handler);
    String nextLogName();
    String lastLogName();
    /**
     * without suffixe
     **/
    long lastWriteLogName(boolean realTime);
    File   nextLogFile(Cell cell);
    IOHandler ioHandler(String fileName) throws FileNotFoundException;
    /**
     * @param mode  rw,r
     **/
    IOHandler ioHandler(String fileName,String mode) throws FileNotFoundException;
    IOHandler bufferedIOHandler(String fileName, int bufferSize) throws FileNotFoundException;

    /**
     * @param mode
     **/
    IOHandler bufferedIOHandler(String fileName, int bufferSize,String mode) throws FileNotFoundException;
    /**
     *  default 'rw' ,'r','rwd','rds'
     **/
    IOHandler bufferedIOHandler(String fileName,IOHandler handler) throws FileNotFoundException;
    IOHandler bufferedIOHandler(String fileName,IOHandler handler,String mode) throws FileNotFoundException;
    void asyncCloseFileChannel(IOHandler handler);
    List<Long> allLogFiles();
    List<Long> allSortedFiles(String suffix);
    /**
     * log 文件尾部和index块的总大小
     **/
    int  tailerAndIndexSize();

    int  logWritableSize();
    /**
     * 判断是否需要 重放
     **/
    boolean needReplayLog();

    String  fileName(long position);

    /**
     * 计算map 初始化大小
     * */
    int mapInitSize(int expectSize, float loadFactor);


    File file(long fileName,String suffix);
    long addByteSize(long size);

    /**
     *
     * 预计的文件数量
     **/
    int expectedSize();
    /**
     * 目录下文件数量
     **/
    int size();



}
