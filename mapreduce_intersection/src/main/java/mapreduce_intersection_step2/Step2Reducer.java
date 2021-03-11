package mapreduce_intersection_step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Step2Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String value3 = "";
        for (Text text : values){
            value3 += text.toString() + "-";
        }
        context.write(key, new Text(value3.substring(0, value3.length()-1)));
    }
}
