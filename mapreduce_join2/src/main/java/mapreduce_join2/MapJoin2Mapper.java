package mapreduce_join2;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

public class MapJoin2Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private final List<String> localList = new LinkedList<>();

    //读取分布式缓存到本地的 list 集合
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //得到所有缓存文件路径
        URI[] cacheFiles = context.getCacheFiles();
        //通过文件获取文件系统
        FileSystem fileSystem = FileSystem.get(cacheFiles[0], context.getConfiguration());
        //获取文件输入流
        FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));
        //包装成字符缓冲流
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        //读取
        String line = null;
        while((line = bufferedReader.readLine()) != null){
            localList.add(line);
        }
        //关闭
        bufferedReader.close();
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        for (String s : localList){
            if (value.toString().equals(s)){
                context.write(value, new LongWritable(1));
            }
        }
    }
}
