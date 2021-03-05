package zookeeper_api;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

public class ZookeeperAPITest {
    @Test
    public void createZnode() throws Exception {
        //定制重试策略
        //获取一个客户端对象
        //开启客户端
        //创建节点
        //关闭客户端
                                                                //重试间隔时间，        重试次数
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);

        String connectionStr = "192.168.106.129:2181, 192.168.106.130:2181, 192.168.106.131:2181";
        //参数（服务器列表，会话超时时间，链接超时时间，重试策略
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000,
                8000, retryPolicy);
        client.start();
        //     创建节点      如果父节点不存在则创建                          永久节点                路径，数据
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello3",
                "world".getBytes(StandardCharsets.UTF_8));
        client.close();
    }

    @Test
    public void createTmpZnode() throws Exception {
        //重试间隔时间，        重试次数
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);

        String connectionStr = "192.168.106.129:2181, 192.168.106.130:2181, 192.168.106.131:2181";
        //参数（服务器列表，会话超时时间，链接超时时间，重试策略
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000,
                8000, retryPolicy);
        client.start();
        //     创建节点      如果父节点不存在则创建                          临时节点                路径，数据
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/tmp5",
                "world".getBytes(StandardCharsets.UTF_8));
        client.close();
    }

    @Test
    public void setZnodeData() throws Exception {
        //重试间隔时间，        重试次数
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);

        String connectionStr = "192.168.106.129:2181, 192.168.106.130:2181, 192.168.106.131:2181";
        //参数（服务器列表，会话超时时间，链接超时时间，重试策略
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000,
                8000, retryPolicy);
        client.start();

        client.setData().forPath("/hello", "zookeeper".getBytes(StandardCharsets.UTF_8));

        client.close();
    }

    @Test
    public void getZnodeData() throws Exception {
        //重试间隔时间，        重试次数
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);

        String connectionStr = "192.168.106.129:2181, 192.168.106.130:2181, 192.168.106.131:2181";
        //参数（服务器列表，会话超时时间，链接超时时间，重试策略
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000,
                8000, retryPolicy);
        client.start();

        byte[] bytes = client.getData().forPath("/hello");
        System.out.println(new String(bytes));

        client.close();
    }

    @Test
    public void watchNode() throws Exception{
        //重试间隔时间，        重试次数
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);

        String connectionStr = "192.168.106.129:2181, 192.168.106.130:2181, 192.168.106.131:2181";
        //参数（服务器列表，会话超时时间，链接超时时间，重试策略
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000,
                8000, retryPolicy);
        client.start();

        //关联目录
        TreeCache treeCache = new TreeCache(client, "/hello2");
        //自定义监听器
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework,
                                   TreeCacheEvent treeCacheEvent) throws Exception {
                ChildData data = treeCacheEvent.getData();
                if (data != null){
                    switch (treeCacheEvent.getType()){
                        case NODE_ADDED: System.out.println("新增节点"); break;
                        case NODE_REMOVED: System.out.println("删除节点"); break;
                        case NODE_UPDATED: System.out.println("更新节点"); break;
                        default: break;
                    }
                }
            }
        });
        //开始监听
        treeCache.start();
        Thread.sleep(100000);
    }
}
