package hbase_mapred_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class FruitReducer extends TableReducer<LongWritable, Text, NullWritable> {

    private String cf = null;
    private String cn1 = null;
    private String cn2 = null;

    //获取参数
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        cf = configuration.get("cf");     //在 main 中 set
        cn1 = configuration.get("cn1");
        cn2 = configuration.get("cn2");
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values){
            String[] fields = value.toString().split("\t");
            Put put = new Put(Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn1), Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn2), Bytes.toBytes(fields[2]));

            context.write(NullWritable.get(), put);
        }
    }
}
