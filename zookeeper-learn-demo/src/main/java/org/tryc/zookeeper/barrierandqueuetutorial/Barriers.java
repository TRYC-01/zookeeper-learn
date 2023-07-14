package org.tryc.zookeeper.barrierandqueuetutorial;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class Barriers extends SyncPrimitive {

    private String name;

    private int size;


    public Barriers(String address, String root, int size) {
        super(address);
        this.root = root;
        this.size = size;
        // Create barrier node
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                System.out.println("Keeper exception when instantiating queue: " + e.toString());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // My node name
        try {
            name = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

    }

    boolean enter() throws InterruptedException, KeeperException {
        // 临时有序zonde,相同的name创建不会报错
        zk.create(root + "/" + name, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        while (true) {
            synchronized (mutex) {
                List<String> children = zk.getChildren(root, true);
                if (children.size() < size) {
                    mutex.wait();
                } else {
                    return true;
                }
            }
        }
    }

    boolean leave() throws InterruptedException, KeeperException {
        zk.delete(root + "/" + name, 0);
        while (true) {
            synchronized (mutex) {
                List<String> children = zk.getChildren(root, true);
                if (children.size() > 0) {
                    mutex.wait();
                } else {
                    return true;
                }
            }
        }
    }

}
