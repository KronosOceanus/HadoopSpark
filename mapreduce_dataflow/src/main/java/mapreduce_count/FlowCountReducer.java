package mapreduce_count;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Integer upFlow = 0;
        Integer downFlow = 0;
        Integer upCountFlow = 0;
        Integer downCountFlow = 0;

        for(FlowBean o : values){
            upFlow += o.getUpFlow();
            downFlow += o.getDownFlow();
            upCountFlow += o.getUpCountFlow();
            downCountFlow += o.getDownCountFlow();
        }

        FlowBean o = new FlowBean();
        o.setUpFlow(upFlow);
        o.setDownFlow(downFlow);
        o.setUpCountFlow(upCountFlow);
        o.setDownCountFlow(downCountFlow);

        context.write(key, o);
    }
}
