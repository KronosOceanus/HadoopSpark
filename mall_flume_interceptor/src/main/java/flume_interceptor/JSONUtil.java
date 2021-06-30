package flume_interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;

public class JSONUtil {

    public static boolean validateJSON(String log){
        try{
            JSON.parse(log);
            return true;
        }catch (JSONException e){
            return false;
        }
    }
}
