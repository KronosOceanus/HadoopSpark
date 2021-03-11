package untils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * 生成数据
 * 序号，手机号，mac 地址，ip 地址，四个数字
 */
public class Untils {

    private final static int DATA_SIZE = 50;

    //存储对应关系
    private static List<String> mapReaction = new LinkedList<>();
    public static void main(String[] args) throws Exception{
        BufferedWriter bw = new BufferedWriter(new FileWriter("data_flow.dat"));

        //生成每一行数据
        for(int i=0; i<DATA_SIZE; i++){
            int status = getNum(0, 10);
            //序号，电话号，mac 地址，ip，是对应关系
            String reaction = null;
            //随机生成
            if (status < 5 || mapReaction.isEmpty()){
                reaction = String.valueOf(i) + "\t" + getPhoneNumber() + "\t" +
                        getMac() + "\t" + getIp();
                mapReaction.add(reaction);
            }else {
                int index = getNum(0, mapReaction.size()-1);
                reaction = mapReaction.get(index);
            }
            //直接拼接 upFlow 等属性，写入文件
            reaction += "\t" + getNum(0, 100) + "\t" + getNum(0, 200) + "\t" +
                    getNum(0, 10000) + "\t" + getNum(0, 10000);

            //写入文件
            bw.write(reaction+"\n");
        }

        bw.close();
    }




    private static int getNum(int start,int end) {
        return (int)(Math.random() * (end-start+1) + start);
    }

    //随机生成手机号码
    private static String[] phonePres = "134,135,136,137,138,139".split(",");
    private static String getPhoneNumber(){
        int index = getNum(0, phonePres.length-1);
        // + 10000 是为了填充 0
        return phonePres[index] + String.valueOf(getNum(1,888)+10000).substring(1) +
                String.valueOf(getNum(1,9100)+10000).substring(1);
    }

    //随机生成 mac 地址
    private static String SEPARATOR_OF_MAC = "-";
    private static String getMac() {
        Random random = new Random();
        String[] mac = {
                String.format("%02x", random.nextInt(0xff)),
                String.format("%02x", random.nextInt(0xff)),
                String.format("%02x", random.nextInt(0xff)),
                String.format("%02x", random.nextInt(0xff)),
                String.format("%02x", random.nextInt(0xff)),
                String.format("%02x", random.nextInt(0xff))
        };
        return String.join(SEPARATOR_OF_MAC, mac);
    }

    //随机生成 ip 地址
    private static String getIp() {
        // ip范围
        int[][] range = {
                {607649792, 608174079}, // 36.56.0.0-36.63.255.255
                {1038614528, 1039007743}, // 61.232.0.0-61.237.255.255
                {1783627776, 1784676351}, // 106.80.0.0-106.95.255.255
                {2035023872, 2035154943}, // 121.76.0.0-121.77.255.255
                {2078801920, 2079064063}, // 123.232.0.0-123.235.255.255
                {-1950089216, -1948778497}, // 139.196.0.0-139.215.255.255
                {-1425539072, -1425014785}, // 171.8.0.0-171.15.255.255
                {-1236271104, -1235419137}, // 182.80.0.0-182.92.255.255
                {-770113536, -768606209}, // 210.25.0.0-210.47.255.255
                {-569376768, -564133889}, // 222.16.0.0-222.95.255.255
        };

        Random random = new Random();
        int index = random.nextInt(10);
        String ip = num2ip(range[index][0] + new Random().nextInt(range[index][1] - range[index][0]));
        return ip;
    }
    /*
     * 将十进制转换成IP地址
     */
    private static String num2ip(int ip) {
        int[] b = new int[4];
        String ipStr = "";
        b[0] = (int) ((ip >> 24) & 0xff);
        b[1] = (int) ((ip >> 16) & 0xff);
        b[2] = (int) ((ip >> 8) & 0xff);
        b[3] = (int) (ip & 0xff);
        ipStr = Integer.toString(b[0]) + "." + Integer.toString(b[1]) + "." + Integer.toString(b[2]) + "." + Integer.toString(b[3]);

        return ipStr;
    }
}
