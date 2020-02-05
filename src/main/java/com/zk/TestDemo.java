package com.zk;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestDemo {

    private ZooKeeper zooKeeper;

    /**
     * zookeeper的连接是非阻塞连接，因为zookeeper网络通信框架用的是netty
     * 而netty底层用的是NIO通信
     */

    //建立与zookeeper服务器的连接
    @Before
    public void connect() throws Exception {

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        zooKeeper = new ZooKeeper("192.168.1.130:2181", //ip：端口
                30000,  //超时时间，单位毫秒
                new Watcher() { //监听器
                    public void process(WatchedEvent event) {
                        //SyncConnected表示链接成功事件
                        if(event.getState().equals(Event.KeeperState.SyncConnected)){
                            System.out.println("连接zookeeper服务器成功！");
                            countDownLatch.countDown();
                        }
                    }
                });
        countDownLatch.await();

    }

    /**
     * 创建节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void create() throws KeeperException, InterruptedException {
        zooKeeper.create("/part01",
                "hello".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    /**
     * 获取指定节点的数据
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void getData() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData("/part01",null,null);
        System.out.println(new String(data));
    }

    /**
     * 更新节点数据
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void setData() throws KeeperException, InterruptedException {
        //参数：
        // path：路径
        // data：数据
        // version：数据版本号，每更新一次版本号递增1次 初始值为0，一般情况写-1，表示无论版本号是多少都会更新
        zooKeeper.setData("/part01", "hello zookeeper".getBytes(), -1);
    }

    /**
     * 删除节点
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void delete() throws KeeperException, InterruptedException {
        zooKeeper.delete("/part01",-1);
    }


    /**
     * 获取节点下所有子节点的名字
     */
    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> paths = zooKeeper.getChildren("/part01",null);
        for (String path: paths) {
            System.out.println(path);
        }
    }


    /**
     * 监听节点数据发生变化
     */
    @Test
    public void watchDataChanged() throws KeeperException, InterruptedException {

        for (;;){
            final CountDownLatch countDownLatch = new CountDownLatch(1);

            zooKeeper.getData("/part01", new Watcher() {
                public void process(WatchedEvent event) {
                    if (event.getType().equals(Event.EventType.NodeDataChanged)){
                        System.out.println("该节点有数据发生变化！");
                        try {
                            byte[] data = zooKeeper.getData("/part01", null, null);
                            System.out.println("当前数据为："+new String(data));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        countDownLatch.countDown();
                    }
                }
            }, null);

            countDownLatch.await();
        }
    }

    /**
     * 监听节点被删除事件
     */
    @Test
    public void watchNodeDelete() throws KeeperException, InterruptedException {
        zooKeeper.exists("/part01", new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getType().equals(Event.EventType.NodeDeleted)){
                    System.out.println("节点被删除！");
                }
            }
        });
        while (true);
    }


    /**
     * 监听子节点是否会发生变化
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void watchChildrenNodeChanged() throws KeeperException, InterruptedException {
        for (;;){
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            zooKeeper.getChildren("/part01", new Watcher() {
                public void process(WatchedEvent event) {
                    if(event.getType().equals(Event.EventType.NodeChildrenChanged)){
                        System.out.println("有子节点发生变化");
                        countDownLatch.countDown();
                    }
                }
            });
            try {
                List<String> paths = zooKeeper.getChildren("/part01",null);
                for (String path : paths){
                    path= "/part01/"+path;
                    zooKeeper.getData(path, new Watcher() {
                        public void process(WatchedEvent event) {
                            if(event.getType().equals(Event.EventType.NodeDataChanged)){
                                System.out.println("数据发生变化！");
                                countDownLatch.countDown();
                            }
                        }
                    }, null);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            countDownLatch.await();
        }
    }

}
