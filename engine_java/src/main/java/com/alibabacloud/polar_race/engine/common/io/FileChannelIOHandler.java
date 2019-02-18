package com.alibabacloud.polar_race.engine.common.io;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
public class FileChannelIOHandler implements IOHandler {
    private final static Logger logger= LoggerFactory.getLogger(FileChannelIOHandler.class);
    private File file;
    private String fileName;
    protected RandomAccessFile randomAccessFile;
    protected FileChannel fileChannel;
    private long length;
    private  static int writeCount=0;
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
        try {
            this.length = fileChannel.size();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public FileChannel getFileChannel(){
        return fileChannel;
    }
    @Override
    public void append(ByteBuffer buffer) throws IOException {
        int remain=buffer.remaining();
        while(buffer.hasRemaining()) {
             this.fileChannel.write(buffer);
        }
        updateLength(length,remain);
    }

    /**
     * @param position 指定问位置写入
     * @param add   写入大小
     **/
    private void updateLength(long position,int add ){
            long writeFinalPosition=position+add;
            length=writeFinalPosition>length?writeFinalPosition:length;
    }

    @Override
    public void write(long position, ByteBuffer buffer) throws IOException {
            int remain=buffer.remaining();
            if(buffer.hasRemaining()){
                this.fileChannel.position(position);
                while(buffer.hasRemaining())
                    this.fileChannel.write(buffer);
            }
            updateLength(position,remain);
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

    /***
     * 并发问题
     **/
    @Override
    public int read(long position, ByteBuffer toBuffer) throws IOException {
        return fileChannel.read(toBuffer,position);
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
        return length;
    }

    @Override
    public void close() throws IOException {
        closeFileChannel(false);
    }

    @Override
    public String name() {
        return fileName.substring(0,fileName.indexOf('.'));
    }

    @Override
    public void closeFileChannel(boolean write) throws IOException {
        if(fileChannel.isOpen()) {
            // flush os level page cache,ensure all the change has persistent,
            // fsync
//            if(write)
//                fileChannel.force(true);
            fileChannel.close();
            //randomAccessFile.getFD().sync();
            randomAccessFile.close();
            // 且不再需要
            //fileChannel.truncate()
        }
    }

    @Override
    public void dontNeed(long offset,long len) throws IOException{
        NativeIO.posixFadvise(randomAccessFile.getFD(), offset, len,NativeIO.POSIX_FADV_DONTNEED);
    }

    @Override
    public FileDescriptor fileDescriptor() throws IOException {
        return randomAccessFile.getFD();
    }

    @Override
    public void truncate(long size) throws IOException {
        fileChannel.truncate(size);
        this.length=size;
    }

    @Override
    public boolean delete() {
        return this.file.delete();
    }
}
