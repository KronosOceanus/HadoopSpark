package dao;

import constants.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 发布微博
 * 关注用户
 * 取关用户
 * 获取用户初始化页面
 * 获取用户微博详情
 */
public class HBaseDao {

    /**
     * 发布微博
     *
     * content 以 uid_ts 作为 rowKey（活着为了以后方便获取最新数据，可以使用 uid_9999999999999-ts）
     * 向 content put 数据
     *
     * 从 relation get 到该 uid 对应的 fans
     * 对于每个 fans，向 inbox put uid 发布的数据的 rowKey（一对多，使用 list）
     * 如果有 fans，再操作
     */
    public static void publishWeibo(String uid, String content) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //content
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        long ts = System.currentTimeMillis();   //时间戳
        String contentRowKey = uid + "_" + ts;

        Put contentPut = new Put(Bytes.toBytes(contentRowKey));
        contentPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), //列族
                Bytes.toBytes("content"),   //列名
                Bytes.toBytes(content));    //内容
        contentTable.put(contentPut);


        //inbox
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        Get relationGet = new Get(Bytes.toBytes(uid));
        relationGet.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2)); //列族
        Result relationResult = relationTable.get(relationGet);

        ArrayList<Put> inboxPutList = new ArrayList<>();
        for (Cell cell : relationResult.rawCells()){
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));  //列名即 fans 的 uid（inbox 的 rowKey）
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF),
                    Bytes.toBytes(uid),
                    Bytes.toBytes(contentRowKey)); //为 fans 添加更新的 content
            inboxPutList.add(inboxPut);
        }

        if (! inboxPutList.isEmpty()){  //有粉丝
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPutList);   //写入数据

            inboxTable.close();
        }

        relationTable.close();
        contentTable.close();
        connection.close();
    }

    /**
     * 关注用户
     *
     * 向 relation 双向 put（对于一个表的 put 都添加到一个 list 中）
     *
     * 对于每个 attend，scan relation，并将 scan 到的数据添加时间戳（版本控制），向 inbox put
     * 如果 scan 不为空，再操作（防止未选择关注者直接点关注，的情况）
     */
    public static void addAttends(String uid, String... attends) throws IOException {
        if (attends.length <= 0){
            System.out.println("请选择关注的人");
            return;
        }


        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //relation
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        ArrayList<Put> relationPutList = new ArrayList<>();
        Put uidPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends){
            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1),
                    Bytes.toBytes(attend),
                    Bytes.toBytes(attend));

            Put attendPut = new Put(Bytes.toBytes(attend)); //被关注者 fans + 1
            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2),
                    Bytes.toBytes(uid),
                    Bytes.toBytes(uid));
            relationPutList.add(attendPut);
        }
        relationPutList.add(uidPut);

        relationTable.put(relationPutList);


        //content
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        Put inboxPut = new Put(Bytes.toBytes(uid));
        for (String attend : attends){
            Scan contentScan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
            ResultScanner contentResultScanner = contentTable.getScanner(contentScan);
            long ts = System.currentTimeMillis();

            for (Result result : contentResultScanner){
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF),
                        Bytes.toBytes(attend),
                        ts ++,  //版本控制
                        result.getRow());
            }
        }

        if (! inboxPut.isEmpty()){
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPut);

            inboxTable.close();
        }

        contentTable.close();
        relationTable.close();
        connection.close();
    }

    /**
     * 取关用户
     * 与关注用户相似
     */
    public static void deleteAttends(String uid, String... dels) throws IOException {
        if (dels.length <= 0){
            System.out.println("请选择取关的人");
            return;
        }

        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //relation
        Table relationTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        ArrayList<Delete> relationDeleteList = new ArrayList<>();
        Delete uidDelete = new Delete(Bytes.toBytes(uid));
        for (String del : dels){
            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF1),
                    Bytes.toBytes(del));

            Delete delDelete = new Delete(Bytes.toBytes(del));
            delDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF2),
                    Bytes.toBytes(uid));
            relationDeleteList.add(delDelete);
        }
        relationDeleteList.add(uidDelete);

        relationTable.delete(relationDeleteList);


        //inbox
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));
        for (String del : dels){
            inboxDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF),
                    Bytes.toBytes(del));
        }
        inboxTable.delete(inboxDelete);

        inboxTable.close();
        relationTable.close();
        connection.close();
    }

    /**
     * 获取用户初始化页面
     */
    public static void getInit(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        Table contentTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions(Constants.INBOX_TABLE_VERSIONS);
        Result inboxResult = inboxTable.get(inboxGet);

        for (Cell cell : inboxResult.rawCells()){
            Get contentGet = new Get(CellUtil.cloneValue(cell));
            Result contentResult = contentTable.get(contentGet);
            for (Cell contentCell : contentResult.rawCells()) { //遍历三个版本
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(contentCell)) +
                        "\tCF:" + Bytes.toString(CellUtil.cloneFamily(contentCell)) +
                        "\tCN:" + Bytes.toString(CellUtil.cloneQualifier(contentCell)) +
                        "\tValue:" + Bytes.toString(CellUtil.cloneValue(contentCell)));
            }
        }

        contentTable.close();
        inboxTable.close();
        connection.close();
    }

    /**
     * 获取用户微博详情
     */
    public static void getWeibo(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        Table table = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Scan scan = new Scan();
        RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL,
                new SubstringComparator(uid + "_"));    //过滤器！！！
        scan.setFilter(rowFilter);
        ResultScanner rs = table.getScanner(scan);

        for (Result result : rs){
            for (Cell cell : result.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        "\tCF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        "\tCN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "\tValue:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
}
