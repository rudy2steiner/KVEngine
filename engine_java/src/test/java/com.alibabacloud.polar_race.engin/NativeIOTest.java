package com.alibabacloud.polar_race.engin;

import com.alibabacloud.polar_race.engine.common.io.NativeIO;
import com.sun.jna.Platform;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

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
}