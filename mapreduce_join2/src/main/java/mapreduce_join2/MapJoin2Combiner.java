package mapreduce_join2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MapJoin2Combiner extends Reducer<Text, LongWritable, Text, LongWritable> {

    //规约成集合
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;

        for (LongWritable value : values){
            count += value.get();
        }
        context.write(key, new LongWritable(count));
    }
}
