package hdfs_api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

public class HdfsAPITest {

    @Test
    public void visitByUrl() throws Exception{
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

        InputStream inputStream = new URL("hdfs://node1:8020/a.txt").openStream();
        FileOutputStream outputStream = new FileOutputStream(new File("download/hello.txt"));

        IOUtils.copy(inputStream, outputStream);

        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }

    @Test
    public void getFileSystem1() throws Exception{
        Configuration configuration = new Configuration();
        //文件系统类型
        configuration.set("fs.defaultFS", "hdfs://node1:8020");
        //或者使用 FileSystem.newInstance(configuration)
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println(fileSystem);

        fileSystem.close();
    }

    @Test
    public void getFileSystem2() throws Exception{
        //或者使用 FileSystem.newInstance(new URI("hdfs://node1:8020"),
        //                new Configuration())
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),
                new Configuration());
        System.out.println(fileSystem);

        fileSystem.close();
    }




    @Test
    public void listFiles() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),
                new Configuration());
        //迭代器(path -r)
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator =
                fileSystem.listFiles(new Path("/"), true);
        //遍历
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();
            System.out.println(locatedFileStatus.getPath() + "------" +
                    locatedFileStatus.getPath().getName());

            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            System.out.println("blockLength：" + blockLocations.length);
        }

        fileSystem.close();
    }

    //在这里伪造了用户，系统环境变量设置 HADOOP_USER_NAME=root，也可采用以下方法
    //FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),
    //      new Configuration(), "root");
    @Test
    public void mkdirs() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),
                new Configuration());

        boolean mkdirs = fileSystem.mkdirs(new Path("/aaa/bbb/ccc"));
        System.out.println(mkdirs);

        fileSystem.close();
    }




    @Test
    public void getFileToLocal() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),
                new Configuration());

        FSDataInputStream inputStream = fileSystem.open(new Path("/a.txt"));
        FileOutputStream outputStream = new FileOutputStream("download/a.txt");

        IOUtils.copy(inputStream, outputStream);

        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }

    //涉及到 LocalFileSystem 都需要 hadoop.dll（3.0 以上版本用 3.0 的 hadoop.dll 即可）
    @Test
    public void getFileToLocal2() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),
                new Configuration());

        fileSystem.copyToLocalFile(new Path("/a.txt"),
                new Path("download/a.txt"));

        fileSystem.close();
    }

    @Test
    public void putData() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),
                new Configuration());

        fileSystem.copyFromLocalFile(new Path("download/b.txt"),
                new Path("/b.txt"));

        fileSystem.close();
    }




    //本地小文件合并上传
    @Test
    public void mergeFile() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node1:8020"),
                new Configuration());
        FSDataOutputStream outputStream = fileSystem.create(new Path("/big.txt"));

        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        FileStatus[] fileStatuses = local.listStatus(new Path("download"));

        for(FileStatus fileStatus : fileStatuses){
            FSDataInputStream inputStream = local.open(fileStatus.getPath());
            IOUtils.copy(inputStream, outputStream);    //合并

            IOUtils.closeQuietly(inputStream);
        }
        IOUtils.closeQuietly(outputStream);

        local.close();
        fileSystem.close();
    }
}
