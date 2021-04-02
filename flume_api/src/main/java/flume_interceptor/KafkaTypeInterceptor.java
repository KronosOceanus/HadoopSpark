package flume_interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 发送到不同的 topic
 */
public class KafkaTypeInterceptor implements Interceptor {

    //存放事件
    private List<Event> addHeaderEvents;

    @Override
    public void initialize() {
        addHeaderEvents = new ArrayList<>();
    }

    //两个拦截方法，循环调用
    //单个信息，拦截处理
    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        String body = new String(event.getBody());
        //添加头信息
        if (body.contains("hello")){
            //如果要发往 kafka 不同主题，则这里 headers 必须是 （topic，主题名）
            headers.put("topic", "first");
        }else {
            headers.put("topic", "second");
        }
        return event;
    }

    //批量发送，拦截处理
    @Override
    public List<Event> intercept(List<Event> list) {
        //清空集合
        addHeaderEvents.clear();
        for (Event event : list){
            addHeaderEvents.add(intercept(event));
        }
        return addHeaderEvents;
    }

    @Override
    public void close() {

    }

    //静态内部类
    public static class Builder implements Interceptor.Builder{

        //构造拦截器
        @Override
        public Interceptor build() {
            return new KafkaTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
