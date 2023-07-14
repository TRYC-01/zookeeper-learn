package org.tryc.zookeeper.java.example;

public interface DataMonitorListener {

    /**
     * The existence status of the node has changed.
     * @param data
     */
    void exists(byte[] data);

    /**
     * The zookeeper session is no longer valid
     * @param rc
     */
    void closing(int rc);
}
