package mapreduce_splitoutput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream outputStream1 = null;
    private FSDataOutputStream outputStream2 = null;

    public MyRecordWriter(){}

    //有参构造，传入输出流
    public MyRecordWriter(FSDataOutputStream outputStream1, FSDataOutputStream outputStream2) {
        this.outputStream1 = outputStream1;
        this.outputStream2 = outputStream2;
    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String key = text.toString();
        int numKey = Integer.parseInt(key);
        //文件夹分区条件
        if (numKey > 1){
            outputStream1.write(key.getBytes(StandardCharsets.UTF_8));
            outputStream1.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }else {
            outputStream2.write(key.getBytes(StandardCharsets.UTF_8));
            outputStream2.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //两种关闭方法
        outputStream1.close();
        IOUtils.closeStream(outputStream2);
    }
}
