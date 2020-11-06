import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;

import java.util.Collections;
import java.util.List;

public class ServerWatcher implements CuratorWatcher {
    private CuratorFramework currClient;
    private String zkNode;

    boolean single = true;
    boolean isPrimary = true;

    public ServerWatcher(CuratorFramework currClient, String zkNode) {
        this.currClient = currClient;
        this.zkNode = zkNode;
    }

    @Override
    public void process(WatchedEvent watchedEvent) throws Exception {
        System.out.println("ZooKeeper event: " +watchedEvent);

        List<String> children = currClient.getChildren().usingWatcher(this).forPath(zkNode);

        if (children.size() == 1) {
            System.out.println("This is the single server now!");
            single = true;
            isPrimary = true;
        } else {
            single = false;
            Collections.sort(children);
            System.out.println(children.get(0) + "\n" + children.get(1));
        }
    }
}
