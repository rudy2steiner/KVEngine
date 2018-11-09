package com.alibabacloud.polar_race.engine.common.utils;

import java.util.Random;

public class Arrays {

    public void top(short a[],int first,int end,int k){

        if(first<end){
            int partionIndex=partion(a,first,end);
            if(partionIndex==k-1)return;
            else if(partionIndex>k-1)top(a,first,partionIndex-1,k);
            else top(a,partionIndex+1,end,k);
        }

    }

    int partion(short a[],int first,int end){
        int i=first;
        short main=a[end];
        for(int j=first;j<end;j++){
            if(a[j]>main){
                short temp=a[j];
                a[j]=a[i];
                a[i]=temp;
                i++;
            }
        }
        a[end]=a[i];
        a[i]=main;
        return i;
    }

    int partionB(short arr[],int begin,int end){
        int pivotIndex = begin;
        int pivot = arr[pivotIndex];
        swap(arr, pivotIndex, end);

        int low = begin;
        int high = end;

        while (low < high) {
            // 因为把pivot放在了最后，所以low指针先走
            while (low < high && arr[low] < pivot) low++;
            while (low < high && arr[high] > pivot) high--;
            if(low < high) swap(arr, low, high);
        }
        return low;
    }
    public void swap(short[] arr,int i,int j ){
        short tmp=arr[i];
        arr[i]=arr[j];
        arr[j]=tmp;
    }



    public static void  main(String[] args){
        Random random=new Random();
        short[] values=new short[100];
        for(int i=0;i<values.length;i++){
            values[i]=(short)random.nextInt(1000);
        }

        Arrays arrays=new Arrays();
        arrays.top(values,0,values.length-1,10);

        System.out.println(java.util.Arrays.toString(values));
        java.util.Arrays.sort(values);
        System.out.println(java.util.Arrays.toString(values));
        //988, 976, 990, 969, 960, 953, 965, 951, 931, 928

        //928, 931, 951, 953, 960, 965, 969, 976, 988, 990
    }

}
