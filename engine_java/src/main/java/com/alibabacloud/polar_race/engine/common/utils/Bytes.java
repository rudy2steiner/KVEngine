package com.alibabacloud.polar_race.engine.common.utils;

public class Bytes {
    public static void long2bytes(long v, byte[] b, int off) {
        b[off + 7] = (byte) v;
        b[off + 6] = (byte) (v >>> 8);
        b[off + 5] = (byte) (v >>> 16);
        b[off + 4] = (byte) (v >>> 24);
        b[off + 3] = (byte) (v >>> 32);
        b[off + 2] = (byte) (v >>> 40);
        b[off + 1] = (byte) (v >>> 48);
        b[off + 0] = (byte) (v >>> 56);
    }

    public static void int2bytes(int v, byte[] b, int off) {
        b[off + 3] = (byte) v;
        b[off + 2] = (byte) (v >>> 8);
        b[off + 1] = (byte) (v >>> 16);
        b[off + 0] = (byte) (v >>> 24);
    }
    public static void short2bytes(int v, byte[] b, int off) {
        b[off + 1] = (byte) v;
        b[off + 0] = (byte) (v >>> 8);
    }
    public static int bytes2short(byte[] b, int off) {
        return  ((b[off + 1] & 0xFF) << 0) +
                ((b[off + 0] & 0xFF) << 8);

    }


    public static int bytes2int(byte[] b, int off) {
        return ((b[off + 3] & 0xFF) << 0) +
                ((b[off + 2] & 0xFF) << 8) +
                ((b[off + 1] & 0xFF) << 16) +
                ((b[off + 0] & 0xFF) << 24);
    }


    /**
     * to long.
     *
     * @param b   byte array.
     * @param off offset.
     * @return long.
     */
    public static long bytes2long(byte[] b, int off) {
        return ((b[off + 7] & 0xFFL) << 0) +
                ((b[off + 6] & 0xFFL) << 8) +
                ((b[off + 5] & 0xFFL) << 16) +
                ((b[off + 4] & 0xFFL) << 24) +
                ((b[off + 3] & 0xFFL) << 32) +
                ((b[off + 2] & 0xFFL) << 40) +
                ((b[off + 1] & 0xFFL) << 48) +
                (((long) b[off + 0]) << 56);
    }

    public static int  bitSpace(int value){
        if(value<=0) return 0;
        int bit=0;
        while(value>0){
            value>>>=1;
            bit++;
        }
        return bit-1;
    }

    /**
     * compare unsigned 8 byte of the long, lexicographical
     **/
    public static int compareUnsigned(long left,long right){
            if(left==right) return 0;
        return (left + Long.MIN_VALUE) < (right + Long.MIN_VALUE) ? -1 : 1;
    }



}
