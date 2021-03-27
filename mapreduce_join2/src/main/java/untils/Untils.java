package untils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.net.URI;

/**
 * 生成大小表
 */
public class Untils {

    private final static int DATA_SIZE = 5;

    public static void main(String[] args) throws Exception{
        BufferedWriter bw = new BufferedWriter(new FileWriter("small.txt"));
        BufferedWriter bw2 = new BufferedWriter(new FileWriter("big.txt"));
        for(int i=0; i<DATA_SIZE; i++){
            bw.write(getNum(0, 10) + "\n");
        }

        for(int i=0; i<10*DATA_SIZE; i++){
            bw2.write(getNum(0, 10) + "\n");
        }

        bw.close();
        bw2.close();

        putHdfs("small.txt", "/input/cache2/small.txt");
        putHdfs("big.txt", "/input/mapjoin2/big.txt");
    }


    private static void putHdfs(String src, String desc) throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"), new Configuration());

        fileSystem.copyFromLocalFile(new Path(src), new Path(desc));

        fileSystem.close();
    }


    private static int getNum(int start,int end) {
        return (int)(Math.random() * (end-start+1) + start);
    }
}
