package com.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;

public class TestDemo {


    //建立与zookeeper服务器的连接
    @Test
    public void connect() throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper("192.168.1.130:2181", //ip：端口
                30000,  //超时时间，单位毫秒
                new Watcher() { //监听器
                    public void process(WatchedEvent event) {
                        //SyncConnected表示链接成功事件
                        if(event.getState().equals(Event.KeeperState.SyncConnected)){
                            System.out.println("连接zookeeper服务器成功！");
                        }
                    }
                });
        while(true);
    }



}
