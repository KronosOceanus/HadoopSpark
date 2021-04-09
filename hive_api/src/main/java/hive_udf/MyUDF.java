package hive_udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 获取字符串长度
 */
public class MyUDF extends GenericUDF {
    //校验数据参数个数（参数数组）
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1){
            throw new UDFArgumentException("参数个数不为 1");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    //处理数据（参数数组）
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String input = deferredObjects[0].get().toString();
        if (input == null){
            return 0;
        }else {
            return input.length();
        }
    }

    //显示执行计划（在 hql 中用 explain 关键字查看）
    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
