package hbase_mapred_1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "fruit");

        job.setJarByClass(JobMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://node1:8020/input/hbasemr/fruit.csv"));

        job.setMapperClass(FruitMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        //输出
        TableMapReduceUtil.initTableReducerJob("fruit", FruitReducer.class, job);

        boolean finished = job.waitForCompletion(true);
        return finished ? 0:1;
    }

    public static void main(String[] args) throws Exception{
        //连接 hbase
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node1,node2,node3");

        configuration.set("cf", "info");
        configuration.set("cn1", "name");
        configuration.set("cn2", "color");

        int runStatus = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(runStatus);
    }
}
