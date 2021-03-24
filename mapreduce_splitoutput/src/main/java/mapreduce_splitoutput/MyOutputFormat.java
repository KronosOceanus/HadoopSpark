package mapreduce_splitoutput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutputFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //获取目标文件输出流（两个）
        Path path1 = new Path("hdfs://node1:8020/output/left_out.txt");
        Path path2 = new Path("hdfs://node1:8020/output/right_out.txt");
        //通过上下文获取配置
        FileSystem fileSystem = path1.getFileSystem(taskAttemptContext.getConfiguration());
        FSDataOutputStream outputStream1 = fileSystem.create(path1);
        FSDataOutputStream outputStream2 = fileSystem.create(path2);

        return new MyRecordWriter(outputStream1, outputStream2);
    }
}
