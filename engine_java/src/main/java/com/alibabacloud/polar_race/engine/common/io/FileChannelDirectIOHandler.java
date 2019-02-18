package com.alibabacloud.polar_race.engine.common.io;


import net.smacke.jaydio.DirectRandomAccessFile;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.nio.ByteBuffer;

public class FileChannelDirectIOHandler implements IOHandler {
    DirectRandomAccessFile directRandomAccessFile;
    public FileChannelDirectIOHandler(File file, String mode ) throws IOException{
        directRandomAccessFile=
                new DirectRandomAccessFile(file, "r", 8*1024);
    }
    @Override
    public void append(ByteBuffer buffer) throws IOException {

    }

    @Override
    public void write(long position, ByteBuffer buffer) throws IOException {

    }

    @Override
    public void append(byte[] data) throws IOException {

    }

    @Override
    public void append(byte[] data, int offset, int len) throws IOException {

    }

    @Override
    public void write(long position, byte[] data) throws IOException {

    }

    @Override
    public int read(long position, ByteBuffer toBuffer) throws IOException {
         directRandomAccessFile.seek(position);
         directRandomAccessFile.read(toBuffer.array(),0,toBuffer.capacity());
        toBuffer.position(toBuffer.capacity());
        return toBuffer.capacity();
    }

    @Override
    public int read(ByteBuffer toBuffer) throws IOException {
        return 0;
    }

    @Override
    public void flushBuffer() throws IOException {

    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void flush0() throws IOException {

    }

    @Override
    public long position() throws IOException {
        return 0;
    }

    @Override
    public void position(long position) throws IOException {
        directRandomAccessFile.seek(position);
    }

    @Override
    public ByteBuffer buffer() {
        return null;
    }

    @Override
    public void setBuffer(ByteBuffer buffer) {

    }

    @Override
    public void unBuffer() throws IOException {

    }

    @Override
    public long length() throws IOException {
        return directRandomAccessFile.length();
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public void closeFileChannel(boolean write) throws IOException {

    }

    @Override
    public void dontNeed(long offset, long len) throws IOException {

    }

    @Override
    public FileDescriptor fileDescriptor() throws IOException {
        return null;
    }

    @Override
    public void truncate(long size) throws IOException {

    }

    @Override
    public void close() throws IOException {
        directRandomAccessFile.close();
    }

    @Override
    public boolean delete() {
        return false;
    }
}
