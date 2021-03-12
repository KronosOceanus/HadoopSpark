package mapreduce_combineinput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JobMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "sequence_file_job");

        job.setInputFormatClass(MyInputFormat.class);
        MyInputFormat.addInputPath(job, new Path("hdfs://node1:8020/input/combineinput"));

        job.setMapperClass(SequenceFileMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //不用设置 Reducer，但是要设置数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //二进制文件，一般作为中间文件
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        Path path = new Path("hdfs://node1:8020/output/combineinput_out");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),
                new Configuration());
        boolean exists = fileSystem.exists(path);
        if (exists){
            fileSystem.delete(path, true);
        }
        SequenceFileOutputFormat.setOutputPath(job, path);

        boolean finished = job.waitForCompletion(true);
        return finished ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int runStatus = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(runStatus);
    }
}
