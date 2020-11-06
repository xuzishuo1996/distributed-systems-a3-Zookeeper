import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;

import java.util.Collections;
import java.util.List;

public class ServerWatcher implements CuratorWatcher {
    static Logger log;

    private CuratorFramework currClient;
    private String zkNode;

    boolean single = true;
    boolean isPrimary = true;

    public ServerWatcher(CuratorFramework currClient, String zkNode) {
        this.currClient = currClient;
        this.zkNode = zkNode;

        BasicConfigurator.configure();
        log = Logger.getLogger(ServerWatcher.class.getName());
    }

    @Override
    public void process(WatchedEvent watchedEvent) throws Exception {
        log.info("ZooKeeper event: " +watchedEvent);

        List<String> children = currClient.getChildren().usingWatcher(this).forPath(zkNode);

        if (children.size() == 1) {
            log.info("This is the single server now!");
            single = true;
            isPrimary = true;
        } else {
            single = false;
            Collections.sort(children);
            log.info(children.get(0) + "\n" + children.get(1));
        }
    }
}
