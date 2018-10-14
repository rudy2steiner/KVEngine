package com.alibabacloud.polar_race.engine.common;

public class ArraysComp {

    public static int compare(final byte[] v1,final byte[] v2){
        if (v1==v2)
            return 0;
        int lim = Math.min(v1.length, v2.length);
        int k = 0;
        while (k < lim) {
            byte c1 = v1[k];
            byte c2 = v2[k];
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
        }
        return v1.length - v2.length;
    }
}
