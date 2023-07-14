package org.tryc.zookeeper.java.example;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.*;

/**
 * A simple example program to use DataMonitor to start and
 * stop executables based on a znode. The program watches to the
 * znode in the filesystem. It also starts the specified program
 * with the specified arguments when the znode exists and kills
 * the program if the znode goes away
 */
public class Executor implements Watcher, Runnable, DataMonitorListener {
    private String fileName;
    private String[] exec;

    private ZooKeeper zk;

    private DataMonitor dm;

    private String znode;

    private Process child;


    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.printf("USAGE: Executor hostPort znode filename program [args ...]");
            System.exit(2);
        }
        String hostPort = args[0];
        String znode = args[1];
        String fileName = args[2];
        String[] exec = new String[args.length - 3];
        System.arraycopy(args, 3, exec, 0, exec.length);
        try {
            new Executor(hostPort, fileName, znode, exec).run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public Executor(String hostPort, String fileName, String znode, String[] exec) throws IOException {
        this.fileName = fileName;
        this.exec = exec;
        this.zk = new ZooKeeper(hostPort, 4000, this);
        this.dm = new DataMonitor(zk, znode, null, this);
    }

    public void run() {
        try {
            synchronized (this) {
                while (!dm.isDead()) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * We do process any events ourselves,we just need to forward them on
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        dm.process(event);
    }

    @Override
    public void exists(byte[] data) {
        if (data == null) {
            if (child != null) {
                System.out.println("Killing process");
                child.destroy();
                try {
                    child.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            child = null;
        } else {
            if (child != null) {
                System.out.println("Stopping child");
                child.destroy();
                try {
                    child.waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                FileOutputStream fos = new FileOutputStream(fileName);
                fos.write(data);
                fos.close();

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            System.out.println("Starting child");
            try {
                child = Runtime.getRuntime().exec(exec);
                new StreamWriter(child.getInputStream(), System.out);
                new StreamWriter(child.getErrorStream(), System.out);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void closing(int rc) {
        synchronized (this) {
            notifyAll();
        }
    }

    static class StreamWriter extends Thread {
        OutputStream out;
        InputStream in;

        StreamWriter(InputStream in, OutputStream out) {
            this.out = out;
            this.in = in;
            start();
        }

        public void run() {
            byte[] b = new byte[80];
            int rc;
            try {
                while ((rc = in.read(b)) > 0) {
                    out.write(b, 0, rc);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }
}
