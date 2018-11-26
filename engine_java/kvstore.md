#### 主要设计


1 记录存储record
  {
  "key":"xx",
  "value":"xxx",
  "txId":0
  }
 序列化：len|txId|key|value
        2    4    8  4096 
2 索引设计 index
  {
    "key":"xx",
    "offset":10000,
    "txId":0
   }
    key|offset|txId
    8     8    
3 日志文件设计
  { 
    "version":0, //日志文件格式版本
    "length": 1000,// 整个tail和索引内容的大小
    "recordOffset" : 2111,
    "index":0, //是否存储了索引
   }
    {version|length|record_offset|......}=20
      1       4      
  header|{index|.....}|{record|record......}  
    
备注：
   对于同一个持久化的key,offset可以作为其版本号；
   内存中，ring buffer sequence 作为版本号；
   local jvm:   
   -server -Xms2560m -Xmx2560m -XX:MaxDirectMemorySize=256m -XX:MaxMetaspaceSize=200m -XX:NewRatio=1 -XX:+UseConcMarkSweepGC 
   -XX:+UseParNewGC -XX:-UseBiasedLocking -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDetails -XX:+PrintGCDateStamps 
   -Xloggc:/export/test_gc.log 
   
   -server -Xms2660m -Xmx2660m  -XX:NewSize=900m -XX:MaxNewSize=900m -XX:MaxDirectMemorySize=256m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:-UseBiasedLocking -XX:+PrintGCDetails  -XX:+PrintGCDateStamps
   -Xloggc:/export/test_gc.log  
 -------------
 清理cache 
 
 echo 1 > /proc/sys/vm/drop_caches  
 
 
 cgroup
 
 /sys/fs/cgroup/memory/test
 
 mvn -Dtest=com.alibabacloud.polar_race.engin.EngineTest#benchmark8b4kbWrite
 
  vim src/test/java/com.alibabacloud.polar_race.engin/EngineTest.java 

 
    
   