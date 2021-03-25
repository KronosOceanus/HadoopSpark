package flume_interceptor;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class TypeInterceptor implements Interceptor {


    @Override
    public void initialize() {
        
    }

    @Override
    public Event intercept(Event event) {
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        return null;
    }

    @Override
    public void close() {

    }
}
