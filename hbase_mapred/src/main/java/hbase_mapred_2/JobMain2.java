package hbase_mapred_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain2 extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "fruit2");

        job.setJarByClass(JobMain2.class);

        TableMapReduceUtil.initTableMapperJob("fruit", new Scan(),
                Fruit2Mapper.class, ImmutableBytesWritable.class, Put.class, job);

        //输出
        TableMapReduceUtil.initTableReducerJob("fruit2", Fruit2Reducer.class, job);

        boolean finished = job.waitForCompletion(true);
        return finished ? 0:1;
    }

    public static void main(String[] args) throws Exception{
        //连接 hbase
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node1,node2,node3");
        int runStatus = ToolRunner.run(configuration, new JobMain2(), args);
        System.exit(runStatus);
    }
}
