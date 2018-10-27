package com.alibabacloud.polar_race.engine.common.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileChannelIOHandlerImpl implements IOHandler {
    private String dir;
    private String fileName;
    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private ByteBuffer buffer;
    private boolean useBuf =false;
    /**
     * @param mode random access in 'r' ,'rw'
     *
     */
    public FileChannelIOHandlerImpl(String dir,String fileName,String mode,int bufferSize ) throws FileNotFoundException {
        this.dir=dir;
        this.fileName=fileName;
        this.file=new File(dir+fileName);
        this.randomAccessFile=new RandomAccessFile(file,mode);
        this.fileChannel=randomAccessFile.getChannel();
        if(bufferSize>0) {
            this.buffer = ByteBuffer.allocateDirect(bufferSize);
            useBuf =true;
        }
    }

    /**
     *
     **/
    @Override
    public void append(ByteBuffer buf) throws IOException {
           if(useBuf){
               if(buffer.remaining()>=buf.remaining()){
                   buffer.put(buf);
               }else{
                 int remain= buffer.remaining();
                 ByteBuffer  slice= buf.slice();
                             slice.limit(buf.position()+remain);
                   buffer.put(slice);
                   // flip to readable
                   flushBuffer();
                   // skip
                   buf.position(buf.position()+remain);
                   append(buf);
               }
           }else{
               while(buf.hasRemaining())
                   fileChannel.write(buf);
           }
    }

    @Override
    public void write(long position, ByteBuffer buf) throws IOException {
                 flushBuffer();
                 fileChannel.position(position);
                 while(buf.hasRemaining()){
                     fileChannel.write(buf);
                 }
    }

    @Override
    public int read(long position, ByteBuffer toBuf) throws IOException{
             flushBuffer();
             fileChannel.position(position);
             return fileChannel.read(toBuf);
    }

    @Override
    public int read(ByteBuffer toBuf) throws IOException {
            flushBuffer();
            return fileChannel.read(toBuf);
    }

    public void flushBuffer() throws IOException{
        // has buffer byte
        if(useBuf &&buffer.position()>0){
            buffer.flip();
            while(buffer.hasRemaining()){
                fileChannel.write(buffer);
            }
            buffer.clear();
        }
    }

    @Override
    public void flush0() throws IOException {
         flushBuffer();
         fileChannel.force(false);
    }

    @Override
    public long position() throws IOException{
        return fileChannel.position();
    }

    @Override
    public void position(long nwePosition) throws IOException {
               fileChannel.position(nwePosition);
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public void unBuffer() throws IOException{
        flushBuffer();
        useBuf =false;
    }

    public long length() throws IOException{
        long len=0;
        if(useBuf){
            len= buffer.position();
        }
        return  this.fileChannel.size()+len;
    }

    @Override
    public void flush() throws IOException {
        flushBuffer();
    }

    @Override
    public void close() throws IOException {
         fileChannel.close();
    }
}
