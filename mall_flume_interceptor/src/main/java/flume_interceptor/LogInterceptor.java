package flume_interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class LogInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        if (JSONUtil.validateJSON(log)){
            return event;
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        list.removeIf(event -> intercept(event) == null);
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogInterceptor();
        }

        //获取 flume 配置文件参数
        @Override
        public void configure(Context context) {

        }
    }
}
