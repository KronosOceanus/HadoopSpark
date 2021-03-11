package reduce_join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //判断数据来自哪个文件（不同文件分割策略不同）
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        String[] datas = value.toString().split(",");
        if (fileName.equals("products.txt")){
            context.write(new Text(datas[0]), value);
        }else {
            context.write(new Text(datas[2]), value);
        }
    }
}
