package mapreduce_partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.net.URI;
import java.util.Random;

public class Untils {

    //生成 mapreduce 分区所需数据
    public static void main(String[] args) throws  Exception{
        BufferedWriter out = new BufferedWriter(new FileWriter("part.txt"));
        Random r = new Random();

        int k=10;
        for (int i=1; i<=1000; i++){
            out.write(r.nextInt(20) + "\t");
            if (i % 10 == 0){
                out.write("\n");
            }
        }

        out.close();
    }

    //将文件上传到 hdfs
    @Test
    public void putData() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),
                new Configuration());

        fileSystem.copyFromLocalFile(new Path("part.txt"),
                new Path("/input/partitioner/part.txt"));

        fileSystem.close();
    }
}
