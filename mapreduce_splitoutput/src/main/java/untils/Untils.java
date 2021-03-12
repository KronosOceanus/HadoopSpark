package untils;

import java.io.BufferedWriter;
import java.io.FileWriter;

/**
 * 生成 0~2
 */
public class Untils {

    private final static int DATA_SIZE = 50;

    private static int getNum(int start,int end) {
        return (int)(Math.random() * (end-start+1) + start);
    }

    public static void main(String[] args) throws Exception {
        BufferedWriter bw = new BufferedWriter(new FileWriter("splitoutput.txt"));
        for (int i=0; i<DATA_SIZE; i++){
            bw.write(getNum(0,2) + "\n");
        }
        bw.close();
    }
}
