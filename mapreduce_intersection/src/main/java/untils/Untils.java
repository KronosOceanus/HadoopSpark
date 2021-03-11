package untils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;

/**
 * 生成集合
 * A:B,D,E
 * D:B,C,F
 * 并写入文件
 */
public class Untils {

    private final static int DATA_SIZE = 20;

    private static int getNum(int start,int end) {
        return (int)(Math.random() * (end-start+1) + start);
    }

    private static Map<Character, Set<Character>> getMap(){
        Map<Character, Set<Character>> map = new HashMap<>();
        for (int i=0; i<DATA_SIZE; i++){
            Set<Character> set = new HashSet<>();
            map.put((char)getNum(65, 90), set);
            for (int j=0; j<DATA_SIZE; j++){
                set.add((char)getNum(65, 90));
            }
        }
        return map;
    }

    public static void main(String[] args) throws Exception {
        BufferedWriter bw = new BufferedWriter(new FileWriter("intersection.txt"));
        Map<Character, Set<Character>> map = getMap();
        for (Character key : map.keySet()){
            String line = String.valueOf(key);
            for (Character setValue : map.get(key)){
                line += "," + setValue;
            }
            line += "\n";
            //替换结果在返回值中
            bw.write(line.replaceFirst(",", ":"));
        }
        bw.close();
    }
}
