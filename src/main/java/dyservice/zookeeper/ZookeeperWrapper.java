package dyservice.zookeeper;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author pelu2
 * @date Aug 29, 2012
 */
public class ZookeeperWrapper {
	private static final Logger LOG = LoggerFactory.getLogger(ZookeeperWrapper.class);
	protected volatile ZooKeeper zookeeper;
	private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
	private static final ZookeeperWrapper instance = new ZookeeperWrapper();
	public static final String PATH_DELIMIT = "/";
	private static final int RETRY_TIMES = 3;

	// singleton
	public static ZookeeperWrapper getInstance() {
		return instance;
	}

	protected <E> E executeZKOperate(ZKOperation<E> operation)
			throws KeeperException, InterruptedException {
		KeeperException exception = null;
        for (int i = 1; i <= RETRY_TIMES; i++) {
            try {
                return operation.execute();
            } catch (KeeperException e) {
            	if ((e instanceof KeeperException.SessionExpiredException)
    					|| (e instanceof KeeperException.ConnectionLossException)) {
            		
            		LOG.info("ZK execute " + operation.operationName() + ", at " + i + " times, SessionID" + zookeeper.getSessionId() 
            				+", Now State: " + zookeeper.getState() + ", ZK server throw exception, ", e);
    				createZookeeper(i);
    				exception = e;
    			}else{
    				exception = e;
    			}
            }
        }
        throw exception;
	}

	private synchronized void createZookeeper(int count) {
		if (zookeeper == null) {
			zookeeper = createZK();
			
			if(null != zookeeper){
				LOG.info("init zookeeper finished. status: " + zookeeper.getState() + ", SessionID: " + zookeeper.getSessionId() + ".");
			}else{
				LOG.info("Create ZK instance failed, Zookeeper server is not available, or the session timeout is too short.");
			}
			
		} else {
			try {
				if (count < RETRY_TIMES) {
					LOG.info("The ZK server status is " + zookeeper.getState() + ", count: " + count + ", retry again.");
					return;
				}
				
				// test zookeeper status
				if (zookeeper.getState() == ZooKeeper.States.CONNECTED) {
					LOG.info("The ZK server status is avalable, zk state is " + zookeeper.getState());
					return;
				}

				LOG.info("The ZK server status is unavalable, zk state is " + zookeeper.getState());
				
				zookeeper.exists("/", false);

			} catch (KeeperException e) {
				if ((e instanceof KeeperException.SessionExpiredException)
						|| (e instanceof KeeperException.ConnectionLossException)) {
					try {
						LOG.info("Create ZK instance once more, zk state is " + zookeeper.getState());

						// close old zookeer instance
						zookeeper.close();

						LOG.info("Close the older zk instance. its state is " + zookeeper.getState()  + ", SessionID: " + zookeeper.getSessionId() + ".");
						zookeeper = createZK();
						
						if(null != zookeeper){
							LOG.info("Create one zk instance. its state is " + zookeeper.getState()  + ", SessionID: " + zookeeper.getSessionId() + ".");
						}else{
							LOG.info("Create ZK instance failed, Zookeeper server is not available, or the session timeout is too short.");
						}

					} catch (InterruptedException e1) {
						LOG.error("thread interrupted.", e1);
						Thread.currentThread().interrupt();
					}
				}
			} catch (InterruptedException e2) {
				LOG.error("thread interrupted.", e2);
				Thread.currentThread().interrupt();
			}
		}
	}

	private ZookeeperWrapper() {
		createZookeeper(0);
	}

	private ZooKeeper createZK() {
		
        final CountDownLatch zkConnectLatch = new CountDownLatch(1);
		ZooKeeper zk = null;
		
		try {
			LOG.info("-------create zookeeper instance starting--------");	
			zk = new ZooKeeper(ZookeeperConfigConstant.CONN_STRING, ZookeeperConfigConstant.SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // countdown the latch on all events, even if we haven't
                    // successfully connected.
                    zkConnectLatch.countDown();

                    // TODO: handle session disconnects and expires
            		LOG.info("Trigger the watcher, after create zk instance, wather event: " + event);
            		
                }
            });
			
			if (!zkConnectLatch.await(ZookeeperConfigConstant.SESSION_TIMEOUT, TimeUnit.MILLISECONDS)
		            || (zk.getState() != States.CONNECTED)) {
		            throw KeeperException.create(KeeperException.Code.CONNECTIONLOSS);
		        }
			
			LOG.info("-------create zookeeper instance success, SessionId: " + zk.getSessionId() + "...");		
			return zk;
		} catch (Exception e) {
			LOG.error("-------create zookeeper instance fail--------", e);
			if(null != zk){
				try {
					zk.close();
				} catch (InterruptedException e1) {
					LOG.error("------- close zookeeper instance fail --------", e1);
				}
			}
			return null;
		} 
	}

	private static interface ZKOperation<E> {
		E execute() throws KeeperException, InterruptedException;
		String operationName();
	}



	public String createSequenceNode(final String parentPath, final String path, final byte[] data)
			throws KeeperException, InterruptedException {
		return executeZKOperate(new ZKOperation<String>() {
			@Override
			public String execute() throws KeeperException,
					InterruptedException {
				if(null == zookeeper.exists(parentPath, false)){
					zookeeper.create(parentPath, null, acl, CreateMode.PERSISTENT);
				}
				return zookeeper.create(path, data, acl,
						CreateMode.EPHEMERAL_SEQUENTIAL);
			}

			@Override
			public String operationName() {
				return "createSequenceNode path " + path;
			}

		});
	}

	public String createEphemeralNode(final String path, final byte[] data)
			throws KeeperException, InterruptedException {
		return executeZKOperate(new ZKOperation<String>() {
			@Override
			public String execute() throws KeeperException,
					InterruptedException {
				return zookeeper.create(path, data, acl, CreateMode.EPHEMERAL);
			}

			@Override
			public String operationName() {
				return "createEphemeralNode path " + path;
			}

		});
	}

	public String create(final String path, final byte data[])
			throws KeeperException, InterruptedException {
		return executeZKOperate(new ZKOperation<String>() {
			@Override
			public String execute() throws KeeperException,
					InterruptedException {
				return zookeeper.create(path, data, acl, CreateMode.PERSISTENT);
			}

			@Override
			public String operationName() {
				return "create path " + path;
			}
		});
	}

	public List<String> getChildren(final String path, final boolean watch)
			throws KeeperException, InterruptedException {
		return executeZKOperate(new ZKOperation<List<String>>() {
			@Override
			public List<String> execute() throws KeeperException,
					InterruptedException {
				return zookeeper.getChildren(path, watch);
			}

			@Override
			public String operationName() {
				return "getChildren path: " + path;
			}
		});
	}

	public List<String> getChildren(final String path, final Watcher watch)
			throws KeeperException, InterruptedException {
		return executeZKOperate(new ZKOperation<List<String>>() {
			@Override
			public List<String> execute() throws KeeperException,
					InterruptedException {
				return zookeeper.getChildren(path, watch);
			}
			
			@Override
			public String operationName() {
				return "getChildren path: " + path;
			}
		});
	}

	public long getSessionId() {
		return zookeeper.getSessionId();

	}

	public boolean isAvalable() {
		if (zookeeper.getState() == ZooKeeper.States.CONNECTED) {
			return true;
		} else {
			boolean normal = true;
			try {
				zookeeper.exists("/", false);
			} catch (Exception e) {
				normal = false;
			}
			return normal;
		}
	}

	public States getState(){
		return zookeeper.getState();
	}
	
	public Stat exists(final String path, final Watcher watcher)
			throws KeeperException, InterruptedException {
		return executeZKOperate(new ZKOperation<Stat>() {
			@Override
			public Stat execute() throws KeeperException, InterruptedException {
				return zookeeper.exists(path, watcher);
			}

			@Override
			public String operationName() {
				return "exist path: " + path;
			}
		});
	}

	public Stat exists(final String path, final boolean watcher)
			throws KeeperException, InterruptedException {
		return executeZKOperate(new ZKOperation<Stat>() {
			@Override
			public Stat execute() throws KeeperException, InterruptedException {
				return zookeeper.exists(path, watcher);
			}
			
			@Override
			public String operationName() {
				return "exist path: " + path;
			}
		});
	}

	public void delete(final String path, final int version)
			throws InterruptedException, KeeperException {
		executeZKOperate(new ZKOperation<Object>() {
			@Override
			public Object execute() throws KeeperException,
					InterruptedException {
				zookeeper.delete(path, version);
				return null;
			}
			
			@Override
			public String operationName() {
				return "delete path: " + path;
			}
		});
	}

	public Stat setData(final String path, final byte data[], final int version)
			throws KeeperException, InterruptedException {
		return executeZKOperate(new ZKOperation<Stat>() {
			@Override
			public Stat execute() throws KeeperException, InterruptedException {
				if (data != null && data.length > 1024 * 1024) {
					LOG.warn("setData is very large. path=" + path);
				}
				return zookeeper.setData(path, data, version);
			}
			
			@Override
			public String operationName() {
				return "set data path: " + path;
			}
		});
	}

	/**
	 * Ensures that the given path exists with the given data, ACL and flags
	 * 
	 * @param path
	 * @param acl
	 * @param flags
	 */
	public void ensurePathExists(final String path) throws KeeperException,
			InterruptedException {
		Stat state = exists(path, false);
		if (state != null) {
			return;
		}

		assert path.startsWith(PATH_DELIMIT);
		String tmpPath = path;
		if (path.endsWith(PATH_DELIMIT)) {
			tmpPath = path.substring(0, path.length() - 1);

		}
		SimpleStack<String> unCreatedPathStack = new SimpleStack<String>();
		unCreatedPathStack.push(tmpPath);
		int lastSlashPos = tmpPath.lastIndexOf(PATH_DELIMIT);
		while (lastSlashPos != 0) {
			tmpPath = tmpPath.substring(0, lastSlashPos);
			
			state = exists(tmpPath, false);
			if(state != null){
				break;
			}
			
			unCreatedPathStack.push(tmpPath);
			lastSlashPos = tmpPath.lastIndexOf(PATH_DELIMIT);
		}
		while (!unCreatedPathStack.empty()) {
			try {
				create(unCreatedPathStack.pop(), null);
			} catch (KeeperException.NodeExistsException e) {
				// skip the exception. some case other client create the
				// path simultaneously
			}
		}

	}

	/**
	 * @param siblePath
	 * @param b
	 * @param stat
	 * @return
	 */
	public byte[] getData(final String siblePath, final boolean watch,
			final Stat stat) throws KeeperException, InterruptedException {
		return executeZKOperate(new ZKOperation<byte[]>() {
			@Override
			public byte[] execute() throws KeeperException,
					InterruptedException {
				return zookeeper.getData(siblePath, watch, stat);
			}
			
			@Override
			public String operationName() {
				return "get data path: " + siblePath;
			}
		});
	}

	public byte[] getData(final String path, final Watcher watcher,
			final Stat stat) throws KeeperException, InterruptedException {
		return executeZKOperate(new ZKOperation<byte[]>() {
			@Override
			public byte[] execute() throws KeeperException,
					InterruptedException {
				return zookeeper.getData(path, watcher, stat);
			}
			
			@Override
			public String operationName() {
				return "get data path: " + path;
			}
		});
	}
}
