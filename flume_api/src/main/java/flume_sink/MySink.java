package flume_sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 与自定义 Source 不同的是，事务需要显式调用
 */
public class MySink extends AbstractSink implements Configurable {
    //日志
    private Logger logger =  LoggerFactory.getLogger(MySink.class);

    private String prefix;
    private String suffix;

    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix");
        suffix = context.getString("suffix");
    }

    //轮询 channel 中的事件，批量移除并写入到存储系统
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();    //开启

        try{
            Event event = channel.take(); //获取事件

            if (event != null){
                //处理事件
                String body = new String(event.getBody());
                logger.info(prefix + body + suffix);  //打印

            }

            transaction.commit();
            status = Status.READY;
        }catch (Exception e){
            e.printStackTrace();
            transaction.rollback();
            status = Status.BACKOFF;
        }finally {
            transaction.close();
        }
        return status;
    }

}
