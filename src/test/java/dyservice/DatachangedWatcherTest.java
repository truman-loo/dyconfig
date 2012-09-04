/**
 * 
 */
package dyservice;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * @author pelu2
 * @date Sep 1, 2012
 */
public class DatachangedWatcherTest extends ZookeeperBaseTest {

	private final static String sourceData = "Old";
	private final static String changedData = "New";
	
	/* 
	 * 1. create one node, and set the value of 'Old', 
	 * 2. register one data watcher to this node,
	 * 3. start one thread to change that value to 'New',
	 * 4. get the watcher, check the value whether has change to 'New'.
	 * 
	 */
	@Override
	public void testWatcher() {
		final CountDownLatch watcherLatch = new CountDownLatch(1);

		final String uuid = UUID.randomUUID().toString();
		
		try {
			support.createEphemeralNode("/" + uuid, sourceData.getBytes(charsetName));
			
			byte[] result = support.getData("/" + uuid, new Watcher(){

				@Override
				public void process(WatchedEvent event) {
					watcherLatch.countDown();
				}
			}, null);
			
			String sr = new String(result, charsetName);
			
			if(sr.equals(sourceData)){
				new Thread(new Runnable(){

					@Override
					public void run() {
						try {
							support.setData("/" + uuid, changedData.getBytes(charsetName), -1);
						} catch (Exception e) {
							e.printStackTrace();
						} 
					}}).start();
				
				watcherLatch.await();
				
				result = support.getData("/" + uuid, null, null);
				sr = new String(result, charsetName);
				
				if(sr.equals(changedData)){
					System.out.println(sourceData + " has changed to " + changedData);
				}
			}else{
				throw new RuntimeException("meet exception");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
