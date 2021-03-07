package mapreduce_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, SortBean, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //封装成 SortBean
        String[] data = value.toString().split("\t");
        System.out.println(data.length);
        SortBean o = new SortBean();
        o.setWord(data[0]);
        o.setNum(Integer.parseInt(data[1]));

        context.write(o, NullWritable.get());
    }
}
