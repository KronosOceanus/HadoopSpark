package untils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * 生成订单项
 * 订单 id，商品 id，商品价格（启动商品 id 和商品价格对应）
 */
public class Untils {

    private final static int DATA_SIZE = 100;

    private static int getNum(int start,int end) {
        return (int)(Math.random() * (end-start+1) + start);
    }

    //存储对应关系
    private static List<String> mapReaction = new LinkedList<>();
    public static void main(String[] args) throws Exception {
        BufferedWriter bw = new BufferedWriter(new FileWriter("orderitem.txt"));

        for(int i=0; i<DATA_SIZE; i++){
            int status = getNum(0, 10);
            String reaction = "";

            int orderId = getNum(0, 20);
            if (status < 5 || mapReaction.isEmpty()){
                int productId = getNum(0,50);
                int price = getNum(0,1000);
                reaction = productId + "\t" + price;
                mapReaction.add(reaction);
            }else {
                int index = getNum(0, mapReaction.size()-1);
                reaction = mapReaction.get(index);
            }
            bw.write(orderId + "\t" + reaction + "\n");
        }

        bw.close();
    }
}
