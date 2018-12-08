package com.alibabacloud.polar_race.engin;

import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.FileChannelIOHandler;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.io.NativeIO;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.kv.file.LogFileService;
import com.alibabacloud.polar_race.engine.kv.file.LogFileServiceImpl;
import com.sun.jna.Platform;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.BitSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
@Ignore
public class NativeIOTest {

    @Test
    public void requireThatDropFileFromCacheDoesNotThrow() throws IOException {
        File testFile = new File("/export/testfile");
        FileOutputStream output = new FileOutputStream(testFile);
        output.write('t');
        output.flush();
        output.close();
        NativeIO nativeIO = new NativeIO();
        if (Platform.isLinux()) {
            assertTrue(nativeIO.valid());
        } else {
            assertFalse(nativeIO.valid());
            assertEquals("Platform is unsúpported. Only supported on linux.", nativeIO.getError().getMessage());
        }
        nativeIO.dropFileFromCache(output.getFD());
        nativeIO.dropFileFromCache(testFile);
        testFile.delete();
        nativeIO.dropFileFromCache(new File("file.that.does.not.exist"));
    }

    @Test
    public void mappedByteBufferTest(){
        MappedByteBuffer byteBuffer;


    }

    @Test
    public void transfer(){
        LogFileService logFileService=new LogFileServiceImpl("/export/wal000/5/wal",null);
        List<Long> files=logFileService.allLogFiles();
        String transfer="transfer/";
        Files.makeDirIfNotExist("/export/wal000/5/wal/"+transfer);
        try {
            FileChannelIOHandler ioHandlerOrigion;
            FileChannelIOHandler ioHandler;
            for(int i=0;i<files.size();i++){
                 ioHandlerOrigion = (FileChannelIOHandler) logFileService.ioHandler(files.get(i)+StoreConfig.LOG_FILE_SUFFIX);
                 ioHandler = (FileChannelIOHandler) logFileService.ioHandler(transfer+files.get(i)+"_bak"+StoreConfig.LOG_FILE_SUFFIX);
                for(int k=0;k<506;k++){
                    ioHandlerOrigion.getFileChannel().transferTo(k*StoreConfig.LOG_ELEMENT_SIZE, StoreConfig.LOG_ELEMENT_SIZE,ioHandler.getFileChannel());
                }
            }
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    @Test
    public void Bitset(){
        int size=200000;
        BitSet[] bitSets=new BitSet[size];
        for(int i=0;i<size;i++) {
            bitSets[i] = new BitSet(512);
        }
        try {
            Thread.sleep(5000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }


    }
}