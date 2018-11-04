package com.alibabacloud.polar_race.engine.kv;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.common.StoreConfig;
import com.alibabacloud.polar_race.engine.common.io.IOHandler;
import com.alibabacloud.polar_race.engine.common.utils.Bytes;
import com.alibabacloud.polar_race.engine.common.utils.Files;
import com.alibabacloud.polar_race.engine.kv.event.Put;
import com.alibabacloud.polar_race.engine.kv.index.IndexHashAppender;
import com.alibabacloud.polar_race.engine.kv.index.IndexLogReader;
import com.alibabacloud.polar_race.engine.kv.index.IndexVisitor;
import com.alibabacloud.polar_race.engine.kv.wal.MultiTypeLogAppender;
import com.alibabacloud.polar_race.engine.kv.wal.WalLogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class WALogger implements WALog<Put> {

    private final static Logger logger= LoggerFactory.getLogger(WALogger.class);
    private ThreadLocal<ByteBuffer> keyBuf=new ThreadLocal<>();
    private ThreadLocal<ByteBuffer> valueBuf=new ThreadLocal<>();
    private String walDir;
    private String indexDir;
    private String rootDir;
    private ConcurrentHashMap<Long,ValueIndex> valueIndexMap;
    private LogFileService logFileService;
    private MultiTypeLogAppender appender;
    private IndexLogReader indexLogReader;
    private IndexHashAppender hashIndexAppender;
    public WALogger(String dir){
        this.rootDir=dir;
        this.walDir =dir+StoreConfig.VALUE_CHILD_DIR;
        this.indexDir=dir+StoreConfig.INDEX_CHILD_DIR;
        Files.makeDirIfNotExist(walDir);
        Files.makeDirIfNotExist(indexDir);
        this.logFileService =new LogFileServiceImpl(walDir);
        this.transferIndexToHashLogInit();
    }
    /**
     *  查看是否异常退出，并恢复日志完整性
     *
     **/
    public IOHandler replayLastLog() throws IOException{
        String lastLogName= logFileService.lastLogName();
        if(lastLogName!=null){
            WalLogParser logParser=new WalLogParser(logFileService,lastLogName+StoreConfig.LOG_FILE_SUFFIX);
            ByteBuffer to=ByteBuffer.allocate(logFileService.tailerAndIndexSize());
          return  logParser.doRecover(null,to,null);
        }
        return null;
    }

    /**
     *     将索引hash 到索引文件
     **/
    public void concurrentHashIndex() throws Exception{
        if(logFileService.allLogFiles().size()>0) {
            long start=System.currentTimeMillis();
            hashIndexAppender.start();
            indexLogReader.start();
            indexLogReader.iterate(new IndexVisitor() {
                @Override
                public void visit(ByteBuffer buffer) throws Exception {
                    hashIndexAppender.append(buffer);
                }
            });
            logger.info(String.format("hash index write finish, time %d",System.currentTimeMillis()-start));
            hashIndexAppender.close();
        }
       // valueIndexMap =new ConcurrentHashMap<>(StoreConfig.KEY_INDEX_MAP_INIT_CAPACITY);
    }

    @Override
    public long log(Put event) throws Exception{
           appender.append(event);
           return event.txId();
    }
    @Override
    public void iterate(AbstractVisitor visitor) throws IOException {
        List<Long> logNames= logFileService.allLogFiles();
        LogParser parser;
        ByteBuffer to= ByteBuffer.allocate(StoreConfig.FILE_READ_BUFFER_SIZE);
        ByteBuffer from= ByteBuffer.allocate(StoreConfig.FILE_READ_BUFFER_SIZE);
        for(Long logName:logNames){
            parser=new LogParser(walDir,logName+StoreConfig.LOG_FILE_SUFFIX);
            parser.parse(visitor,to,from);
            to.clear();
            from.clear();
        }
    }


    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) {

    }

    @Override
    public byte[] get(byte[] key) throws Exception{
            ValueIndex index= valueIndexMap.get(Bytes.bytes2long(key,0));
            String fileName= logFileService.fileName(index.getOffset());
            //logger.info(String.format("offset %d find file name %s",index.getOffset(),fileName));
            if(fileName==null) return new byte[0];
            IOHandler handler= logFileService.ioHandler(fileName);
            ByteBuffer keyBuffer=keyBuf.get();
                if(keyBuffer==null) {
                    keyBuffer = ByteBuffer.allocate(8);
                    keyBuf.set(keyBuffer);
                }
                keyBuffer.clear();
            ByteBuffer valueBuffer=valueBuf.get();
                if(valueBuffer==null){
                    valueBuffer=ByteBuffer.allocate(4096);
                    valueBuf.set(valueBuffer);
                }
                valueBuffer.clear();
            long offset=index.getOffset()-Long.valueOf(handler.name())+2;
            handler.position(offset);
            //logger.info(String.format("%s offset %d",fileName,offset));
            //logger.info(String.format("read key:%s offset %d",fileName,handler.position()));
            handler.read(keyBuffer);
            //logger.info(String.format("read value %s offset %d",fileName,handler.position()));
            handler.read(valueBuffer);
        return valueBuffer.array();
    }

    @Override
    public void start() throws Exception{
        IOHandler handler;
        String nextLogName;
        boolean redo=logFileService.needReplayLog();
        if(redo){
            handler=replayLastLog();
            nextLogName= logFileService.nextLogName(handler);
        }else{
            nextLogName= logFileService.nextLogName();
        }
        concurrentHashIndex();
//        handler= logFileService.bufferedIOHandler(nextLogName,StoreConfig.FILE_WRITE_BUFFER_SIZE);
//        this.appender=new MultiTypeLogAppender(handler, logFileService,StoreConfig.DISRUPTOR_BUFFER_SIZE);
//        this.appender.start();
    }

    public void transferIndexToHashLogInit(){
        hashIndexAppender=new IndexHashAppender(indexDir,StoreConfig.HASH_BUCKET_SIZE,StoreConfig.HASH_WRITE_BUFFER_SIZE,StoreConfig.HASH_INDEX_QUEUE_SIZE);
        indexLogReader=new IndexLogReader(walDir,logFileService);
    }

    @Override
    public void close() throws Exception {
         this.appender.close();
         this.hashIndexAppender.close();
    }
}
