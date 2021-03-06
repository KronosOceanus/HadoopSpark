package mapreduce_api;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * @param key K1，数字，行 id
     * @param value V1，一行单词
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text text = new Text();
        LongWritable longWritable = new LongWritable();

        String[] words = value.toString().split(",");
        for (String word : words){
            text.set(word);
            longWritable.set(1);
            //写入 K2,V2
            context.write(text, longWritable);
        }
    }
}
