package mapreduce_topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {

    public GroupComparator() {
        //指定分组根据的类
        super(OrderBean.class, true);
    }

    //指定分组规则，相同 orderId 分组，分组之后同一组只会保留最大（排序规则）的 K2，存在一些数据丢失
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        String orderId1 = ((OrderBean)a).getOrderId();
        String orderId2 = ((OrderBean)b).getOrderId();
        return orderId1.compareTo(orderId2);
    }
}
