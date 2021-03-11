package map_join;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Map<String, String> localMap = new HashMap<>();

    //读取分布式缓存到本地的 map 集合
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //得到所有缓存文件路径
        URI[] cacheFiles = context.getCacheFiles();
        //获取文件系统
        FileSystem fileSystem = FileSystem.get(cacheFiles[0], context.getConfiguration());
        //获取文件输入流
        FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));
        //字符缓冲流
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        //读取
        String line = null;
        while((line = bufferedReader.readLine()) != null){
            String[] datas = line.split(",");
            localMap.put(datas[0], line);
        }
        //关闭
        bufferedReader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] datas = value.toString().split(",");
        String productId = datas[2];
        //商品表数据
        String productValue = localMap.get(productId);
        //拼接
        String valueLine = productValue + "\t" + value.toString();
        context.write(new Text(productId), new Text(valueLine));
    }
}
