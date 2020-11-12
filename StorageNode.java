import java.io.*;
import java.util.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

public class StorageNode {
	static Logger log;

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		log = Logger.getLogger(StorageNode.class.getName());

		if (args.length != 4) {
			System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
			System.exit(-1);
		}

		CuratorFramework curClient = CuratorFrameworkFactory.builder().connectString(args[2])
				.retryPolicy(new RetryNTimes(10, 1000)).connectionTimeoutMs(1000).sessionTimeoutMs(10000).build();

		curClient.start();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				curClient.close();
			}
		});

		KeyValueHandler keyValueHandler = new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]);
		KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(keyValueHandler);
		TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
		TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);
		TServer server = new TThreadPoolServer(sargs);
		log.info("Launching server");

		new Thread(new Runnable() {
			public void run() {
				server.serve();
			}
		}).start();

		// create an ephemeral node in ZooKeeper
		String serverString = args[0] + ":" + args[1];
		String result = curClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(args[3] + "/", serverString.getBytes());
		log.info("Curr Server ID: " + result);	// [main] ERROR StorageNode  - [StorageNode.java]Curr Server ID: /z463xu/0000000008
		String[] splitStr = result.split("/");
		String currServerId = splitStr[splitStr.length - 1];
		log.info("Curr Server ID: " + currServerId);	// 0000000008. works
		keyValueHandler.setCurrServerId(currServerId);

		// set up watcher on the children
		ServerWatcher serverWatcher = new ServerWatcher(curClient, args[3], keyValueHandler);
		curClient.sync(); // sync the zookeeper cluster to make sure the client will get the newest data.
		List<String> children = curClient.getChildren().usingWatcher(serverWatcher).forPath(args[3]);
	}
}
