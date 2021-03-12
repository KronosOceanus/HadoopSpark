package mapreduce_splitoutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * 相当于可以写在不同文件夹的分区
 */
public class JobMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "outputformat_job");

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://node1:8020/input/splitoutput"));

        job.setMapperClass(MyOutputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(MyOutputFormat.class);
        //写入中间状态（执行成功）
        Path path = new Path("hdfs://node1:8020/output/splitoutput_out");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),
                new Configuration());
        boolean exists = fileSystem.exists(path);
        if (exists){
            fileSystem.delete(path, true);
        }
        MyOutputFormat.setOutputPath(job, path);

        boolean finished = job.waitForCompletion(true);
        return finished ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int runStatus = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(runStatus);
    }
}
