package mapreduce_partitioner;

import mapreduce_count.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String line = text.toString();
        for (int j=0;j<100;j++){
            System.out.println(j);
        }
        if (line.startsWith("135")){
            return 0;
        }else if (line.startsWith("136")){
            return 1;
        }else if (line.startsWith("137")){
            return 2;
        }else {
            return 3;
        }
    }
}
