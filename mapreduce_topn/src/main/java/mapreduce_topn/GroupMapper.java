package mapreduce_topn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupMapper extends Mapper<LongWritable, Text, OrderBean, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] datas = value.toString().split("\t");

        OrderBean o = new OrderBean();
        o.setOrderId(datas[0]);
        o.setPrice(Integer.parseInt(datas[2]));

        // K2 是行文本数据
        context.write(o, new Text(value.toString()));
    }
}
