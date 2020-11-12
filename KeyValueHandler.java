import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.curator.framework.*;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class KeyValueHandler implements KeyValueService.Iface {
    // fields in starter code
    private Map<String, String> myMap;
    private Lock mapLock;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;

    // fields add by myself
    Logger log;
    boolean isSingle = true;
    boolean isPrimary = true;
    String currServerId;
    String backupServerId;
    InetSocketAddress backupAddress;    // use for when isPrimary is true;
//    KeyValueService.Client clientToBackUp;      // use for when isPrimary is true;
    ConcurrentLinkedQueue<KeyValueService.Client> clientsQueue = null;

    public Map<String, String> getMyMap() {
        return myMap;
    }
    public void setMyMap(Map<String, String> myMap) {
        this.myMap = myMap;
    }
//    public KeyValueService.Client getClientToBackUp() {
//        return clientToBackUp;
//    }
//    public void setClientToBackUp(KeyValueService.Client clientToBackUp) {
//        this.clientToBackUp = clientToBackUp;
//    }
    public InetSocketAddress getBackupAddress() {
        return backupAddress;
    }
    public void setBackupAddress(InetSocketAddress backupAddress) {
        this.backupAddress = backupAddress;
    }
    public String getBackupServerId() {
        return backupServerId;
    }
    public void setBackupServerId(String backupServerId) {
        this.backupServerId = backupServerId;
    }
    public String getCurrServerId() {
        return currServerId;
    }
    public void setCurrServerId(String currServerId) {
        this.currServerId = currServerId;
    }
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
        mapLock = new ReentrantLock();

        BasicConfigurator.configure();
        log = Logger.getLogger(StorageNode.class.getName());
    }

    public String get(String key) throws org.apache.thrift.TException {
        String ret = myMap.get(key);
        if (ret == null)
            return "";
        else
            return ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        mapLock.lock();
        try {
            myMap.put(key, value);

            // send key to the backup if it exists
            if (!isSingle && isPrimary) {
                KeyValueService.Client clientToBackUp = null;
                while(clientToBackUp == null) {
                    try {
                        clientToBackUp = keyValueHandler.getThriftClient(keyValueHandler.getBackupAddress());
                    } catch (Exception e) {
                    }
                }
                clientToBackUp.put(key, value);
            }
        } finally {
            mapLock.unlock();
        }
    }

    public void forwardMap(Map<String, String> mapFromExistedSever) throws org.apache.thrift.TException {
        mapLock.lock();
        try {
            myMap = new ConcurrentHashMap<>(mapFromExistedSever);
        } finally {
            mapLock.unlock();
        }
    }

    KeyValueService.Client getThriftClient(InetSocketAddress address) {
        try {
            TSocket sock = new TSocket(address.getHostName(), address.getPort());
            TTransport transport = new TFramedTransport(sock);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            return new KeyValueService.Client(protocol);
        } catch (Exception e) {
            log.error("Unable to connect to primary");
            return null;
        }
    }

    InetSocketAddress getAddress(String serverId) throws Exception {
        byte[] data = curClient.getData().forPath(zkNode + "/" + serverId);
        String strData = new String(data);
        String[] server = strData.split(":");
        log.info("Found server " + strData);
        return new InetSocketAddress(server[0], Integer.parseInt(server[1]));
    }
}
