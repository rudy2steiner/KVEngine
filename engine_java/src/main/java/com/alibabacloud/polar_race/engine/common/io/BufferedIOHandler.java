package com.alibabacloud.polar_race.engine.common.io;

import com.alibabacloud.polar_race.engine.common.utils.Files;
import org.omg.CORBA.DATA_CONVERSION;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BufferedIOHandler implements IOHandler {
    private IOHandler handler;
    private int bufferSize;
    private ByteBuffer buf;
    public BufferedIOHandler(IOHandler handler,int bufferSize){
        this.handler=handler;
        this.bufferSize=bufferSize;
        if(bufferSize<0)
            throw new IllegalArgumentException("Illegal initial bufferSize: " +
                        bufferSize);
        this.buf=ByteBuffer.allocateDirect(Files.tableSizeFor(bufferSize));
    }
    public BufferedIOHandler(IOHandler handler,ByteBuffer buffer){
        this.handler=handler;
        this.buf=buffer;
    }





    @Override
    public void append(ByteBuffer buffer) throws IOException {
        if(buf.remaining()>=buffer.remaining()){
            buf.put(buffer);
        }else{
            int remain= buf.remaining();
            ByteBuffer  slice= buffer.slice();
            slice.limit(buffer.position()+remain);
            buf.put(slice);
            // flip to readable
            flushBuffer();
            // skip
            buffer.position(buffer.position()+remain);
            append(buffer);
        }
    }

    @Override
    public void write(long position, ByteBuffer buffer) throws IOException {
           flushBuffer();
           handler.write(position,buffer);
    }

    @Override
    public void append(byte[] data) throws IOException {
         if(buf.remaining()>=data.length){
            buf.put(data);
         }else {
             int remain= buf.remaining();
             buf.put(data,0,remain);
             flushBuffer();
             buf.put(data,remain,data.length-remain);
         }
    }

    @Override
    public void append(byte[] data, int offset, int len) throws IOException {
        if(buf.remaining()>=len){
            buf.put(data,offset,len);
        }else {
            int remain= buf.remaining();
            buf.put(data,offset,remain);
            flushBuffer();
            buf.put(data,offset+remain,len-remain);
        }
    }

    @Override
    public void write(long position, byte[] data) throws IOException {
        flushBuffer();
        write(position,ByteBuffer.wrap(data));
    }

    @Override
    public String name() {
        return handler.name();
    }

    @Override
    public int read(long position, ByteBuffer toBuffer) throws IOException {
        return handler.read(position,toBuffer);
    }

    @Override
    public int read(ByteBuffer toBuffer) throws IOException {
        return handler.read(toBuffer);
    }

    @Override
    public void flushBuffer() throws IOException {
        if(buf.position()>0){
            buf.flip();
            while(buf.hasRemaining()){
                handler.append(buf);
            }
            buf.clear();
        }
    }

    @Override
    public void flush() throws IOException {
          flushBuffer();
    }

    @Override
    public void flush0() throws IOException {
         handler.flush0();
    }

    @Override
    public long position() throws IOException {
        return handler.position();
    }

    @Override
    public void position(long position) throws IOException {
            handler.position(position);
    }

    @Override
    public ByteBuffer buffer() {
        return buf;
    }

    /**
     *
     */
    @Override
    public void setBuffer(ByteBuffer buffer) {
       this.buf=buffer;
    }

    @Override
    public void unBuffer() throws IOException {

    }

    @Override
    public long length() throws IOException {
        return handler.length()+buf.position();
    }

    @Override
    public void close() throws IOException {
        flushBuffer();
        handler.close();
    }
}
