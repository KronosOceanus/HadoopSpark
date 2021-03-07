package mapreduce_partitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, NullWritable> {

    //定义分区规则，返回分区编号
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        //根据行第一个数是否 > 10 分区
        String numStr = text.toString().split("\t")[0];
        if (Integer.parseInt(numStr) >= 10){
            return 1;
        }
        return 0;
    }
}
