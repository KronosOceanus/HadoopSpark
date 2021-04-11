package utils;

import constants.Constants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 创建命名空间
 * 创建表
 * 判断表是否存在
 */
public class HBaseUtil {

    public static void createNameSpace(String nameSpace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();

        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        admin.createNamespace(namespaceDescriptor);

        admin.close();
        connection.close();
    }

    public static void createTable(String tableName, int versions, String... cfs) throws IOException {
        if (cfs.length <= 0){
            System.out.println("请设置列族");
            return;
        }
        if (isTableExist(tableName)){
            System.out.println(tableName + "表已存在");
        }

        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();

        List<ColumnFamilyDescriptor> cfList = new LinkedList<>();
        for (String cf : cfs){
            cfList.add(ColumnFamilyDescriptorBuilder.   //列族描述器
                    newBuilder(cf.getBytes()).
                    setMaxVersions(versions).build());    //设置版本
        }

        TableDescriptor tableDescriptor = TableDescriptorBuilder.   //表描述器
                newBuilder(TableName.valueOf(tableName)).
                setColumnFamilies(cfList).build();  //添加列组

        admin.createTable(tableDescriptor);

        admin.close();
        connection.close();
    }

    private static boolean isTableExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Admin admin = connection.getAdmin();

        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        admin.close();
        connection.close();

        return exists;
    }
}
