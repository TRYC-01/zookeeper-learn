package org.tryc.zookeeper.barrierandqueuetutorial;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.util.List;

public class Queue extends SyncPrimitive {
    public Queue(String address, String name) {
        super(address);
        this.root = name;
        // Create zk node name
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
    }

    boolean produce(int i) throws InterruptedException, KeeperException {
        ByteBuffer b = ByteBuffer.allocate(4);
        byte[] value;
        // Add child with value i
        b.putInt(i);
        value = b.array();
        zk.create(root + "/element", value, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        return true;
    }

    int consume() throws InterruptedException, KeeperException {
        int retValue = -1;
        Stat stat = null;
        // Get the first element available
        while (true) {
            synchronized (mutex) {
                List<String> children = zk.getChildren(root, true);
                if (children.size() == 0) {
                    System.out.println("Going to wait");
                    mutex.wait();
                } else {
                    Integer min = new Integer(children.get(0).substring(7));
                    for (String s : children) {
                        Integer tempValue = new Integer(s.substring(7));
//                        System.out.println("Temporary value: " + tempValue);
                        if (tempValue < min) {
                            min = tempValue;
                        }
                    }
                    System.out.println("Temporary value: " + "/element" + String.format("%010d",min));
                    byte[] b = zk.getData(root + "/element" + String.format("%010d",min), false, stat);
                    zk.delete(root + "/element" + String.format("%010d",min), 0);
                    ByteBuffer buffer = ByteBuffer.wrap(b);
                    retValue = buffer.getInt();
                    return retValue;
                }
            }
        }
    }
}
