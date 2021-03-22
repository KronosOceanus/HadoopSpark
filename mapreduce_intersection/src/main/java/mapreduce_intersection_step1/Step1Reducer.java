package mapreduce_intersection_step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Step1Reducer extends Reducer<Text, Text, Text, Text> {

    // - 连接 V
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String key3 = "";
        for (Text text : values){
            key3 += text.toString() + "-";
        }
        context.write(new Text(key3.substring(0, key3.length()-1)), key);
    }
}
