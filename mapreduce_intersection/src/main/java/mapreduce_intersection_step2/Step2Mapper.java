package mapreduce_intersection_step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text = new Text();

        String[] datas1 = value.toString().split("\t");
        String[] datas2 = datas1[0].split("-");
        Text value2 = new Text(datas1[1]);

        //排序，为了防止出现 A-C / C-A 这种两集合重复情况
        Arrays.sort(datas2);

        //两个元素以上才能组合
        if (datas2.length > 1){
            for (int i=0; i<datas2.length-1; i++){
                for (int j=i+1; j<datas2.length; j++){
                    String key2 = datas2[i] + "-" + datas2[j];
                    text.set(key2);
                    context.write(text, value2);
                }
            }
        }
    }
}
