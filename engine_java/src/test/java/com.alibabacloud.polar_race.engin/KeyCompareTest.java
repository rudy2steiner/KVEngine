package com.alibabacloud.polar_race.engin;

import org.junit.Test;

public class KeyCompareTest {

    @Test
    public void keyCompare(){
        long min=Long.MIN_VALUE;
        long minn=min+1;
        long max=Long.MAX_VALUE;
        System.out.println(min+","+minn+","+(min+Long.MIN_VALUE)+","+(minn+Long.MIN_VALUE)+","+(max+Long.MIN_VALUE));
    }


}
