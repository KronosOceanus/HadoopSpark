package flume_source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    private String prefix;
    private String suffix;

    //配置参数（在配置文件中），即 a1.sources.r1 之后的内容
    @Override
    public void configure(Context context) {
        prefix = context.getString("prefix");
        suffix = context.getString("suffix", "-oceanus");
    }

    //获取数据，封装并传出事件（到下一步），循环调用
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        try{
            //造数据
            for (int i=0; i<5; i++){
                //封装
                SimpleEvent event = new SimpleEvent();
                //从配置文件中传入 prefix（无默认值）和 suffix（有默认值）（全局变量），并将数据转换成字符数组
                event.setBody((prefix + i + suffix).getBytes());
                //传出事件，事务封装在 ChannelProcessor 中
                getChannelProcessor().processEvent(event);
                status = Status.READY;
            }
        }catch (Exception e){
            e.printStackTrace();
            status = Status.BACKOFF;
        }

        //时间间隔
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
