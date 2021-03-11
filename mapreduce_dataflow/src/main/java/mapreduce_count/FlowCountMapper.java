package mapreduce_count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        FlowBean o = new FlowBean();

        String[] datas = value.toString().split("\t");

        text.set(datas[1]);
        o.setUpFlow(Integer.parseInt(datas[4]));
        o.setDownFlow(Integer.parseInt(datas[5]));
        o.setUpCountFlow(Integer.parseInt(datas[6]));
        o.setDownCountFlow(Integer.parseInt(datas[7]));

        context.write(text, o);
    }
}
