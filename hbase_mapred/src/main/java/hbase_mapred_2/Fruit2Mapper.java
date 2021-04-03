package hbase_mapred_2;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Fruit2Mapper extends TableMapper<ImmutableBytesWritable, Put> {

    //在 map 端构建 Put 对象
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Put put = new Put(key.get());   // key 就是 rowKey
        for (Cell cell : value.rawCells()){
            if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){  //判断列名是否为 name
                put.add(cell);  //添加 1 列
            }
        }
        context.write(key, put);
    }
}
