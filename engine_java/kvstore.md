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
    8     8     4
3 日志文件设计
  { 
    "version":0, 
    "length": 1000,
    "recordOffset" : 2111,
    "index":0, //是否存储了索引
   }
    {version|length|record_offset|......} 4k
    
  header|{index|.....}|{record|record......}  
    
   