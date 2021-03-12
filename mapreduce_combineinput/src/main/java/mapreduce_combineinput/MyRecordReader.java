package mapreduce_combineinput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {

    //文件切片，文件系统配置，文件是否读取完毕，文件字节数组，文件系统，文件输入流
    private FileSplit fileSplit = null;
    private Configuration configuration = null;
    private boolean processed = false;
    private BytesWritable bytesWritable = null;
    private FileSystem fileSystem = null;
    private FSDataInputStream inputStream = null;

    //初始化
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fileSplit = (FileSplit)inputSplit;
        configuration = taskAttemptContext.getConfiguration();
    }

    //获取 K1,V1
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (! processed){
            //文件路径（单个文件）
            Path path = fileSplit.getPath();
            //通过 path 获取文件系统（不自定义的话，用 get）
            fileSystem = path.getFileSystem(configuration);

            inputStream = fileSystem.open(path);
            //文件放入字节数组
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            //输入流，字节数组，偏移量，读取长度
            IOUtils.readFully(inputStream, bytes, 0, (int) fileSplit.getLength());

            //字节数组封装成 hadoop 中的格式
            bytesWritable = new BytesWritable();
            bytesWritable.set(bytes, 0, (int) fileSplit.getLength());

            processed = true;
            return true;
        }

        //该文件是否读取完毕
        return false;
    }

    //获取 K1
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    //获取 V1
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
        fileSystem.close();
    }
}
