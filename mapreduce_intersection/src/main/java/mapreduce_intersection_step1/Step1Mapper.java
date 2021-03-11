package mapreduce_intersection_step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Step1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text = new Text();

        String[] datas1 = value.toString().split(":");
        String[] datas2 = datas1[1].split(",");
        Text value2 = new Text(datas1[0]);

        for (String data : datas2){
            text.set(data);
            context.write(text, value2);
        }
    }
}
