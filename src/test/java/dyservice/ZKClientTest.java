/**
 * 
 */
package dyservice;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import dyservice.zookeeper.ZookeeperWrapper;

/**
 * @author pelu2
 * @date Aug 29, 2012
 */
public class ZKClientTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ZookeeperWrapper support = ZookeeperWrapper.getInstance();
		try {
			support.exists("/test", new Watcher(){

				@Override
				public void process(WatchedEvent event) {
					System.out.println("Triiger " +event);
					
				}
				
			});
			
			support.create("/test", null);
			
			Thread.sleep(1 * 1000);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
