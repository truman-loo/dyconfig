/**
 * 
 */
package dyservice;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

/**
 * @author pelu2
 * @date Sep 1, 2012
 */
public class ExistWatcherTest extends ZookeeperBaseTest {

	/*
	 * 1. check if exist one node and rigester one watcher,
	 * 2. create that node,
	 * 3. trigger the watcher.
	 * 
	 */
	@Override
	public void testWatcher() {
		final CountDownLatch watcherLatch = new CountDownLatch(1);

		final String uuid = UUID.randomUUID().toString();

		/**
		 * this thread is for rigester one exist watcher.
		 */

		try {
			Stat existStat = support.exists("/" + uuid, new Watcher() {

				@Override
				public void process(WatchedEvent event) {
					watcherLatch.countDown();
				}
			});

			if (null == existStat) {
				System.out.println("Path /" + uuid + " is not exist on ZK, need wait for the watcher");
				
				/**
				 * create one path to rigger that watcher.
				 */
				new Thread(new Runnable() {

					@Override
					public void run() {
						try {
							support.createEphemeralNode("/" + uuid, null);
							System.out.println("Create path /" + uuid + " successfully.");
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}).start();
				
				watcherLatch.await();
			} else {
				throw new RuntimeException("Watcher test failed.");
			}

			existStat = support.exists("/" + uuid, false);

			if (null != existStat) {
				System.out.println("/" + uuid + " has exist on ZK.");
			} else {
				throw new RuntimeException("Watcher test failed.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
