package com.alibabacloud.polar_race.engin;


import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Null;
import org.junit.Ignore;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

@Ignore
public class RingBufferTest {

    @Test
    public void test(){

    int v=98;
    byte[] a=new byte[4];
        Bytes.int2bytes(v,a,0);
     System.out.println(Bytes.bytes2int(a,0));
    }

    @Test
    public void binaySearch(){
        List<String> list=new ArrayList();
        list.add("2");
        list.add("4");
        list.add("7");
        list.add("9");
        int mid;
        long value=10;
        long midValue=-1;
        if(!Null.isEmpty(list)){
            int low=0;
            int high=list.size()-1;
            while(low<high){
                 mid=low+(high-low)/2;
                 midValue=Long.valueOf(list.get(mid));
                 if(midValue<value){
                     if(Long.valueOf(list.get(mid+1))>value)
                         break;
                     low=mid+1;
                 }else if(midValue>value){
                     high=mid-1;
                 }else break;

            }
            if(low==high) midValue=Long.valueOf(list.get(high));
            System.out.println(midValue);
        }
    }
}
