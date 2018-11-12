package com.alibabacloud.polar_race.engine.common.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileChannelIOHandler implements IOHandler {

    private File file;
    private String fileName;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    /**
     * @param mode random access in 'r' ,'rw'
     *
     *
     **/
    public FileChannelIOHandler(File file,String mode) throws FileNotFoundException {
        this.file=file;
        this.fileName=file.getName();
        this.randomAccessFile=new RandomAccessFile(file,mode);
        this.fileChannel=randomAccessFile.getChannel();
    }
    @Override
    public void append(ByteBuffer buffer) throws IOException {
        while(buffer.hasRemaining())
            this.fileChannel.write(buffer);
    }

    @Override
    public void write(long position, ByteBuffer buffer) throws IOException {
            if(buffer.hasRemaining()){
                this.fileChannel.position(position);
                append(buffer);
            }
    }

    @Override
    public void append(byte[] data) throws IOException {
         append(ByteBuffer.wrap(data));
    }

    @Override
    public void append(byte[] data, int offset, int len) throws IOException {
        append(ByteBuffer.wrap(data,offset,len));
    }

    @Override
    public void write(long position, byte[] data) throws IOException {
          write(position,ByteBuffer.wrap(data));
    }

    @Override
    public int read(long position, ByteBuffer toBuffer) throws IOException {
                fileChannel.position(position);
                return read(toBuffer);
    }

    @Override
    public int read(ByteBuffer toBuffer) throws IOException {
        return  fileChannel.read(toBuffer);
    }

    @Override
    public void flushBuffer() throws IOException {

    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public void flush0() throws IOException {
              fileChannel.force(false);
    }

    @Override
    public long position() throws IOException {
        return fileChannel.position();
    }

    @Override
    public void position(long position) throws IOException {
        fileChannel.position(position);
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
        return fileChannel.size();
    }

    @Override
    public void close() throws IOException {
        closeFileChannel();
    }

    @Override
    public String name() {
        return fileName.substring(0,fileName.indexOf('.'));
    }

    @Override
    public void closeFileChannel() throws IOException {
        if(fileChannel.isOpen()) {
            // flush os level page cache,possible no work
            fileChannel.force(true);
            fileChannel.close();
            //randomAccessFile.getFD().sync();
            randomAccessFile.close();
        }
    }
}
