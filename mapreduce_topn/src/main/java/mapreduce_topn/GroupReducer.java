package mapreduce_topn;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupReducer extends Reducer<OrderBean, Text, Text, NullWritable> {

    private final int N = 1;

    // values 集合中前 N 个即 topN
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for (Text text : values){
            context.write(text, NullWritable.get());
            i ++;
            if (i >= N){
                break;
            }
        }
    }
}
