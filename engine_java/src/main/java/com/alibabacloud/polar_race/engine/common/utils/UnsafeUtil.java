package com.alibabacloud.polar_race.engine.common.utils;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class UnsafeUtil {

        public static final Unsafe UNSAFE;    static {
            try {
                Field field = Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                UNSAFE = (Unsafe) field.get(null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public static void main(String[] arg) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(4 * 1024 * 1024);
            long addresses = ((DirectBuffer) buffer).address();
            byte[] data = new byte[4 * 1024 * 1024];
            UNSAFE.copyMemory(data, 16, null, addresses, 4 * 1024 * 1024);
        }


        public void append(ByteBuffer buffer,byte[] value,int offset,int len){



        }
}
