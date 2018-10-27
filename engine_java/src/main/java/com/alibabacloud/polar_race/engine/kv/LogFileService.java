package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.io.IOHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

public interface LogFileService {
    String nextLogName(Cell cell);
    File   nextLogFile(Cell cell);
    IOHandler ioHandler(String fileName) throws FileNotFoundException;
    IOHandler bufferedIOHandler(String fileName, int bufferSize) throws FileNotFoundException;
    IOHandler bufferedIOHandler(String fileName,IOHandler handler) throws FileNotFoundException;
    List<String> allLogFiles();
}
