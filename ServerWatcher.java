import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;

import java.util.Collections;
import java.util.List;

public class ServerWatcher implements CuratorWatcher {
    Logger log;

    private CuratorFramework currClient;
    private String zkNode;
    private KeyValueHandler keyValueHandler;

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
    public void process(WatchedEvent watchedEvent) throws Exception {
        log.error("ZooKeeper event: " + watchedEvent);

        List<String> children = currClient.getChildren().usingWatcher(this).forPath(zkNode);

        if (children.size() == 1) {
            log.info("This is the single server now!");
            keyValueHandler.setSingle(true);
            keyValueHandler.setPrimary(true);
        } else {
            keyValueHandler.setSingle(false);
            Collections.sort(children);
            log.error(children.get(0) + "\n" + children.get(1));
            if (keyValueHandler.currServerId.equals(children.get(0))) {
                keyValueHandler.setPrimary(true);
                log.info("This is the primary server now!");
            } else {
                keyValueHandler.setPrimary(false);
                log.info("This is the backup server now!");
            }
        }
    }
}
