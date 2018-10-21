package com.alibabacloud.polar_race.engine.kv;

import com.alibabacloud.polar_race.engine.common.io.IOHandler;

import java.io.FileNotFoundException;
import java.util.List;

public interface LogFileService {
    String nextLogName(Cell cell);
    IOHandler getFileChannelIOHandler(String fileName, int bufferSize) throws FileNotFoundException;
    List<String> allLogFiles();
}
