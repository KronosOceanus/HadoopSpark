package hive_udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 实现 explode
 * 以 , 分割
 * 一进多出（还可以 explode 成多个列）
 */
public class MyUDTF extends GenericUDTF {

    private ArrayList<String> outputList = new ArrayList<>();   //输出数据

    //初始化
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<String> fieldNames = new ArrayList<>();    //执行函数之后的默认列名，可以被别名覆盖
        fieldNames.add("word1");
        fieldNames.add("word2");
        List<ObjectInspector> fieldOIs = new ArrayList<>();     //返回值的数据类型
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    //处理数据
    @Override
    public void process(Object[] objects) throws HiveException {
        String input = objects[0].toString();
        String[] words = input.split(":");
        for (String word : words){
            outputList.clear();
            String[] ws = word.split(",");
            outputList.add(ws[0]);
            outputList.add(ws[1]);
            forward(outputList);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
