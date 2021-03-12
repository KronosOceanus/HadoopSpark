package mapreduce_combineinput;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 每个文件内容作为一个 Bytes 数组
 */
public class MyInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        MyRecordReader myRecordReader = new MyRecordReader();
        //传参
        myRecordReader.initialize(inputSplit, taskAttemptContext);
        return myRecordReader;
    }

    //文件是否可以被切割
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return super.isSplitable(context, filename);
    }
}
