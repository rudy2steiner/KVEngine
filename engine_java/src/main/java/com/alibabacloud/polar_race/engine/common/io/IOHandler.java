package com.alibabacloud.polar_race.engine.common.io;

import java.io.Closeable;
import java.io.FileDescriptor;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface IOHandler extends Flushable, Closeable {
    /**
     *
     * append to end of the file sequentially
     *
     */
    void append(ByteBuffer buffer) throws IOException;
    /**
     *
     * write out the @param buf  beginning from position
     *
     */
    void write(long position,ByteBuffer buffer) throws IOException;


    /**
     *
     * append to end of the file sequentially
     *
     */
    void append(byte[] data) throws IOException;

    /**
     *
     * append to end of the file sequentially
     *
     */
    void append(byte[] data,int offset,int len) throws IOException;
    /**
     *
     * write out the @param buf  beginning from position
     *
     */
    void write(long position,byte[]  data) throws IOException;


    /**
     * read  data into @param toBuffer  beginning from position
     */
    int read(long position,ByteBuffer toBuffer) throws IOException;

    int read(ByteBuffer toBuffer) throws IOException;
    /**
     *
     * read data into @param toBuffer  beginning from default position
     *
     **/
    void flushBuffer() throws IOException;

    /**
     * flush to page cache
     */
    void flush() throws IOException;

    /**
     * flush to disk,flush page cache to disk
     */
    void flush0() throws IOException;

    /**
     *  read or write position of underlying file now
     */
    long position() throws IOException;

//    void asyncClose();
    /**
     * update to new read or write position of underlying file
     **/
    void position(long position) throws IOException;

    /**
     * @return  underlying buffer
     **/
    ByteBuffer buffer();

    /**
     * @deprecated
     */
    void setBuffer(ByteBuffer buffer) ;
    /**
     * @deprecated
     */
    void unBuffer() throws IOException;
    /**
     * @return underlying content length
     **/
    long length() throws IOException;
    /**
     * @return underlying file name,without format suffix
     **/
    String name();

    /**
     * asyncClose underlying file channel
     **/
    void closeFileChannel(boolean write) throws IOException;

    /**
     *  such as fadvise ,implement by linux native api
     *
     **/
    void dontNeed(long offset,long len) throws IOException;

    FileDescriptor fileDescriptor() throws IOException;

    void truncate(long size) throws IOException;
}
