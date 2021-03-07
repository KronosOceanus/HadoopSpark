package mapreduce_partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartitionerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //计数器
        Counter counter = context.getCounter("MR_COUNTER", "partitioner_counter");
        counter.increment(1L);

        context.write(value, NullWritable.get());
    }
}
