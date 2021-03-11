package mapreduce_intersection_step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * 求两个集合的交集
 */
public class JobMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), "mapreduce_intersecion_step2");

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://node1:8020/output/intersection_step1_out"));

        job.setMapperClass(Step2Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Step2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        Path path = new Path("hdfs://node1:8020/output/intersection_step2_out");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),
                new Configuration());
        boolean exists = fileSystem.exists(path);
        if (exists){
            fileSystem.delete(path, true);
        }
        TextOutputFormat.setOutputPath(job, path);

        boolean finished = job.waitForCompletion(true);
        return finished ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int runStatus = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(runStatus);
    }
}
