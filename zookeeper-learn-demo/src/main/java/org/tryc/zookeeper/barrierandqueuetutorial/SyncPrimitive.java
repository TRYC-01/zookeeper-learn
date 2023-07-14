package org.tryc.zookeeper.barrierandqueuetutorial;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Random;

public class SyncPrimitive implements Watcher {

    static ZooKeeper zk = null;
    static Integer mutex;

    String root;

    public SyncPrimitive(String address) {
        if (zk == null) {
            System.out.println("Starting ZK:");
            try {
                zk = new ZooKeeper(address, 4000, this);
                mutex = new Integer(-1);
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
                e.printStackTrace();
                zk = null;
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notifyAll();
        }
    }

    public static void main(String[] args) {
        if (args[0].equals("qTest")) {
            queueTest(args);
        } else {
            barrierTest(args);
        }
    }

    /**
     * bTest 192.168.183.139:2181 5
     * @param args
     */
    private static void barrierTest(String[] args) {
        Barriers b = new Barriers(args[1],"/b1",new Integer(args[2]));
        try {
            boolean flag = b.enter();
            System.out.println("Entered barrier: " + args[2]);
            if (!flag) {
                System.out.println("Error when entering the barrier");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        // Generate random integer
        Random random = new Random();
        int r = random.nextInt(100);
        // Loop for rand iterations
        for (int i = 0; i < r; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {

            }
        }
        try {
            b.leave();
        } catch (InterruptedException e) {

        } catch (KeeperException e) {

        }
        System.out.println("Left barrier");
    }

    /**
     * qTest 192.168.183.139 20 p(c)
     * @param args
     */
    private static void queueTest(String[] args) {
        Queue q = new Queue(args[1], "/app1");
        System.out.println("Input: " + args[1]);
        int i;
        Integer max = new Integer(args[2]);
        if (args[3].equals("p")) {
            System.out.println("Producer");
            for (i = 0; i < max; i++) {
                try {
                    q.produce(10 + i);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println("Consumer");
            for (i = 0; i < max; i++) {
                try {
                    int r = q.consume();
                    System.out.println("Item: " + r);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                    i--;
                }
            }
        }
    }
}
