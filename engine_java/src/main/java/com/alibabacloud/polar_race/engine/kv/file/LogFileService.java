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
    File   nextLogFile(Cell cell);
    IOHandler ioHandler(String fileName) throws FileNotFoundException;
    /**
     * @param mode  rw,r
     **/
    IOHandler ioHandler(String fileName,String mode) throws FileNotFoundException;
    IOHandler bufferedIOHandler(String fileName, int bufferSize) throws FileNotFoundException;
    IOHandler bufferedIOHandler(String fileName,IOHandler handler) throws FileNotFoundException;
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
}
