package test;

import constants.Constants;
import dao.HBaseDao;
import utils.HBaseUtil;

import java.io.IOException;

public class TestWeibo {

    public static void init() throws IOException {
        HBaseUtil.createNameSpace(Constants.NAMESPACE);

        HBaseUtil.createTable(Constants.CONTENT_TABLE,
                Constants.CONTENT_TABLE_VERSIONS,
                Constants.CONTENT_TABLE_CF);
        HBaseUtil.createTable(Constants.RELATION_TABLE,
                Constants.RELATION_TABLE_VERSIONS,
                Constants.RELATION_TABLE_CF1,Constants.RELATION_TABLE_CF2);
        HBaseUtil.createTable(Constants.INBOX_TABLE,
                Constants.INBOX_TABLE_VERSIONS,
                Constants.INBOX_TABLE_CF);
    }


    public static void main(String[] args) throws IOException, InterruptedException {
//        init();
        HBaseDao.publishWeibo("1001", "1001微博一");
        HBaseDao.addAttends("1002", "1001","1003");
        HBaseDao.getInit("1002");
        System.out.println("------------------------------------------------------");
        HBaseDao.publishWeibo("1003", "1003微博一");
        HBaseDao.publishWeibo("1001", "1001微博二");
        HBaseDao.publishWeibo("1003", "1003微博二");
        HBaseDao.publishWeibo("1001", "1001微博三");
        HBaseDao.publishWeibo("1003", "1003微博三");
        HBaseDao.getInit("1002");
        System.out.println("------------------------------------------------------");
        HBaseDao.deleteAttends("1002", "1003");
        HBaseDao.getInit("1002");
        System.out.println("------------------------------------------------------");
        HBaseDao.addAttends("1002", "1003");
        HBaseDao.getInit("1002");
        System.out.println("------------------------------------------------------");
        HBaseDao.getWeibo("1001");
    }
}
