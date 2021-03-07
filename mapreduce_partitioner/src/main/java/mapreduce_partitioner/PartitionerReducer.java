package mapreduce_partitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PartitionerReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    //计数器
    public static enum Counter{
        MY_INPUT_RECORDS, MY_INPUT_BYTES
    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.getCounter(Counter.MY_INPUT_RECORDS).increment(1L);

        context.write(key, NullWritable.get());
    }
}
