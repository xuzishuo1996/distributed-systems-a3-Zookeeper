import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.zookeeper.WatchedEvent;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ServerWatcher implements CuratorWatcher {
    Logger log;

    private CuratorFramework currClient;
    private String zkNode;
    private KeyValueHandler keyValueHandler;

    private static final int SIZE_OF_CLIENTS_QUEUE = 32;

//    boolean single = true;
//    boolean isPrimary = true;

    public ServerWatcher(CuratorFramework currClient, String zkNode, KeyValueHandler keyValueHandler) {
        this.currClient = currClient;
        this.zkNode = zkNode;
        this.keyValueHandler = keyValueHandler;

        BasicConfigurator.configure();
        log = Logger.getLogger(ServerWatcher.class.getName());
    }

    @Override
    synchronized public void process(WatchedEvent watchedEvent) throws Exception {
        log.info("ZooKeeper event: " + watchedEvent);

        currClient.sync(); // sync the zookeeper cluster to make sure the client will get the newest data.
        List<String> children = currClient.getChildren().usingWatcher(this).forPath(zkNode);

        // works
        if (children.size() == 1) {
            log.info("This is the single server now!");
            keyValueHandler.setSingle(true);
            keyValueHandler.setPrimary(true);
        } else {
            keyValueHandler.setSingle(false);
            Collections.sort(children);
            log.info(children.get(0) + "\n" + children.get(1));
            if (keyValueHandler.currServerId.equals(children.get(0))) { // curr server is the primary server
                log.info("This is the primary server now!");
                keyValueHandler.setPrimary(true);
                keyValueHandler.setBackupServerId(children.get(1));
                InetSocketAddress newBackupAddress = keyValueHandler.getAddress(children.get(1));
                keyValueHandler.setBackupAddress(newBackupAddress);

                if (keyValueHandler.clientsQueue == null || !keyValueHandler.backupAddress.equals(newBackupAddress)) {
                    keyValueHandler.clientsQueue = new ConcurrentLinkedQueue<>();

                    KeyValueService.Client clientToBackUp = keyValueHandler.getThriftClient(keyValueHandler.getBackupAddress());
//                    KeyValueService.Client clientToBackUp = null;
//                    while(clientToBackUp == null) {
//                        try {
//                            clientToBackUp = keyValueHandler.getThriftClient(keyValueHandler.getBackupAddress());
//                        } catch (Exception e) {
//                        }
//                    }
                    // forward the whole map to the newly added server
                    clientToBackUp.forwardMap(keyValueHandler.getMyMap());
                    // create clients in a queue for forwarding key-value pairs to the back
                    for (int i = 0; i < SIZE_OF_CLIENTS_QUEUE; ++i) {
                        KeyValueService.Client client = keyValueHandler.getThriftClient(keyValueHandler.getBackupAddress());
                        keyValueHandler.clientsQueue.add(client);
                    }
                }
//                else {
//                    KeyValueService.Client clientToBackUp = null;
//                    while(clientToBackUp == null) {
//                        clientToBackUp = keyValueHandler.clientsQueue.poll();
//                    }
//                    // forward the whole map to the newly added server
//                    clientToBackUp.forwardMap(keyValueHandler.getMyMap());
//                    keyValueHandler.clientsQueue.offer(clientToBackUp);
//                }

            } else {     // curr server is the backup server
                log.info("This is the backup server now!");
                keyValueHandler.setPrimary(false);
                keyValueHandler.setBackupServerId(keyValueHandler.currServerId);
                keyValueHandler.setBackupAddress(null); // only for primary to use
                keyValueHandler.clientsQueue = null;

                // forward the whole map to the newly added server
                InetSocketAddress newServerAddress = keyValueHandler.getAddress(children.get(0));
                KeyValueService.Client client = keyValueHandler.getThriftClient(newServerAddress);
                client.forwardMap(keyValueHandler.getMyMap());
            }
        }
    }
}
