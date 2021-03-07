package mapreduce_api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JobMain extends Configured implements Tool {

    //一个 job 任务，比如 WordCount
    @Override
    public int run(String[] strings) throws Exception {
        //创建 job 任务，获取 psvm 中定义的 configuration，指定 job 名称
        Job job = Job.getInstance(super.getConf(), "wordcount");

        //如果 jar 包运行出错，需额外配置 job
        job.setJarByClass(JobMain.class);

        //配置 job 任务
        //输入类型，文件路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://node1:8020/wordcount"));
        //本地运行则改为如下路径
//        TextInputFormat.addInputPath(job,
//                new Path("C:\\MyProject\\HadoopSpark\\mapreduce_data\\input"));
        // map 阶段使用类，以及 K2,V2 类型
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //规约
        job.setCombinerClass(MyCombiner.class);

        // reduce 阶段使用类，以及 K3,V3 类型
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //输出类型，文件路径（这个路径不能存在，如果存在要先删除）
        job.setOutputFormatClass(TextOutputFormat.class);
        Path path = new Path("hdfs://node1:8020/wordcount_out");
        //改进，如果目录存在则删掉
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),
                new Configuration());
        boolean exists = fileSystem.exists(path);
        if (exists){
            fileSystem.delete(path, true);
        }
        TextOutputFormat.setOutputPath(job, path);
//        TextOutputFormat.setOutputPath(job,
//                new Path("C:\\MyProject\\HadoopSpark\\mapreduce_data\\output"));

        //等待任务结束
        boolean finished = job.waitForCompletion(true);
        return finished ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        //调用 run 方法，并且将 configuration 传递给 Configured 类
        int runStatus = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(runStatus);
    }
}
