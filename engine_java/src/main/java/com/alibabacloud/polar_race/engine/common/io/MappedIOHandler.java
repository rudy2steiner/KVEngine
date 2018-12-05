package com.alibabacloud.polar_race.engine.common.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

public class MappedIOHandler extends FileChannelIOHandler {

    MappedByteBuffer mappedByteBuffer;
    private ReentrantLock lock=new ReentrantLock();
    public MappedIOHandler(File file, String mode,long position,long size) throws FileNotFoundException {
        super(file,mode);
        try {
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, size);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void append(ByteBuffer buffer) throws IOException {
        lock.lock();
        mappedByteBuffer.put(buffer);
        lock.unlock();
    }

    @Override
    public void write(long position, ByteBuffer buffer) throws IOException {
       ByteBuffer slice=mappedByteBuffer.slice();
       slice.position((int)position);
       slice.put(buffer);
    }

    /**
     *
     * slice size byte buffer at the position
     *
     **/
    public ByteBuffer slice(int position,int size){
        ByteBuffer slice=mappedByteBuffer.slice();
        slice.position(position);
        slice.limit(position+size);
        return slice.slice();
    }
}
