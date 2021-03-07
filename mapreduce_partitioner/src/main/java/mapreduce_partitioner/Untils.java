package mapreduce_partitioner;

import java.io.BufferedWriter;
import java.io.FileWriter;
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
}
