package com.alibabacloud.polar_race.engine.common.io;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface IOHandler {
    void append(ByteBuffer buf) throws IOException;
    void write(long position,ByteBuffer buf) throws IOException;
    int read(long position,ByteBuffer toBuf) throws IOException;
    int read(ByteBuffer toBuf) throws IOException;
    void flushBuf() throws IOException;
    boolean flush0() throws IOException;
    long position() throws IOException;
    void position(long position) throws IOException;
    ByteBuffer getBuffer();
    void setBuffer(ByteBuffer buffer) ;
    void unBuffer() throws IOException;
    long length() throws IOException;
}
