package com.alibabacloud.polar_race.engine.common.io;

import java.io.*;
import java.lang.reflect.Field;
import java.util.List;
import java.util.logging.Logger;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.utils.Null;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.file.LogFileServiceImpl;
import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Platform;

/**
 * Provides functionality only possible through native C library.
 */
public class NativeIO {
    private final static Logger logger = Logger.getLogger(NativeIO.class.getName());

    public static final int POSIX_FADV_DONTNEED = 4; // See /usr/include/linux/fadvise.h
    public static final int POSIX_FADV_NORMAL=0;   //POSIX_FADV_NORMAL


    private static boolean initialized = false;
    private static Throwable initError = null;
    static {
        try {
            if (Platform.isLinux()) {
                Native.register(Platform.C_LIBRARY_NAME);
                initialized = true;
            }
        } catch (Throwable throwable) {
            initError = throwable;
        }
    }

    private static final Field fieldFD = getField(FileDescriptor.class, "fd");


    private static native int posix_fadvise(int fd, long offset, long len, int flag) throws LastErrorException;

    public NativeIO() {
        if (!initialized) {
            logger.warning("native IO not possible due to " + getError().getMessage());
        }
    }

    public boolean valid() { return initialized; }
    public Throwable getError() {
        if (initError != null) {
            return initError;
        } else {
            return new RuntimeException("Platform is unsúpported. Only supported on linux.");
        }
    }

    /**
     * Will hint the OS that this is will not be accessed again and should hence be dropped from the buffer cache.
     * @param fd The file descriptor to drop from buffer cache.
     * @param  length 0 表示整个文件
     */
    public void dropFileFromCache(FileDescriptor fd) {
        try {
            fd.sync();
        } catch (SyncFailedException e) {
            logger.warning("Sync failed while dropping cache: " + e.getMessage());
        }
        if (initialized) {
            posix_fadvise(getNativeFD(fd), 0, 0, POSIX_FADV_DONTNEED);
        }
    }

    /***
     *  no need any more
     *  http://hadoop.apache.org/docs/stable1/api/constant-values.html#org.apache.hadoop.io.nativeio.NativeIO.POSIX_FADV_WILLNEED
     *  flush buffer and cache for the file
     *  extending for len bytes (or until the end of the file if len is 0)
     *
     **/
    public static void posixFadvise(FileDescriptor fd,long  offset,long length,int flag){
        posix_fadvise(getNativeFD(fd), offset,length, flag);
    }


    /**
     * Will hint the OS that this is will not be accessed again and should hence be dropped from the buffer cache.
     * @param file File to drop from buffer cache
     */
    public void dropFileFromCache(File file) {
        try {
            dropFileFromCache(new FileInputStream(file).getFD());
        } catch (FileNotFoundException e) {
            logger.fine("No point in dropping a non-existing file from the buffer cache: " + e.getMessage());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static Field getField(Class<?> clazz, String fieldName) {
        Field field;
        try {
            field = clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        field.setAccessible(true);
        return field;
    }

    private static int getNativeFD(FileDescriptor fd) {
        try {
            return fieldFD.getInt(fd);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception{
        LogFileService logFileService = new LogFileServiceImpl("/export/wal000/wal", null);
        List<Long> files = logFileService.allLogFiles();
        RandomAccessFile randomAccessFile;
        for (long fid : files) {
            File file = logFileService.file(fid, StoreConfig.LOG_FILE_SUFFIX);
            if (!Null.isEmpty(file)) {
                randomAccessFile = new RandomAccessFile(file, "rw");
                NativeIO.posix_fadvise(getNativeFD(randomAccessFile.getFD()), 0, file.length(), 4);
            }
        }

    }

}
