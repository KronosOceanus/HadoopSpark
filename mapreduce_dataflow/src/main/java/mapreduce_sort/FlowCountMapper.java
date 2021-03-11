package mapreduce_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FlowBean o = new FlowBean();
        Text text = new Text();

        String[] datas = value.toString().split("\t");

        text.set(datas[0]);
        o.setUpFlow(Integer.parseInt(datas[1]));
        o.setDownFlow(Integer.parseInt(datas[2]));
        o.setUpCountFlow(Integer.parseInt(datas[3]));
        o.setDownCountFlow(Integer.parseInt(datas[4]));

        context.write(o, text);
    }
}
