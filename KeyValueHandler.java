import java.util.*;
import java.util.concurrent.*;

import org.apache.curator.framework.*;

public class KeyValueHandler implements KeyValueService.Iface {
    // fields in starter code
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;

    // fields add by myself
    boolean isSingle = true;
    boolean isPrimary = true;

    public boolean isSingle() {
        return isSingle;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public void setSingle(boolean single) {
        this.isSingle = single;
    }

    public void setPrimary(boolean primary) {
        isPrimary = primary;
    }

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<>();
    }

    public String get(String key) throws org.apache.thrift.TException {
        String ret = myMap.get(key);
        if (ret == null)
            return "";
        else
            return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        myMap.put(key, value);

        // send key to the backup if it exists
    }
}
