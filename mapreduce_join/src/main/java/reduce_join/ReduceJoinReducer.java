package reduce_join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String first = null;
        String second = null;
        for (Text text : values){
            //搞好顺序
            if (text.toString().startsWith("p")){
                first = text.toString();
            }else {
                second = text.toString();
            }
        }

        context.write(key, new Text(first + "\t" + second));
    }
}
