/**
 * 
 */
package dyservice;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import com.cisco.dynconfig.zookeeper.ZookeeperWrapper;

/**
 * @author pelu2
 * @date Aug 29, 2012
 */
public class DynConfigService {
	public static ZookeeperWrapper support = ZookeeperWrapper.getInstance();

	public static void main(String...strings){
	    final CountDownLatch zkConnectLatch = new CountDownLatch(1);
	    
	    final String uuid = UUID.randomUUID().toString();
	    
	    /**
	     * this thread is for rigester one exist watcher.
	     */
	    new Thread(new Runnable(){

			@Override
			public void run() {
				try{
					Stat existStat = support.exists("/" + uuid, new Watcher(){

						@Override
						public void process(WatchedEvent event) {
							zkConnectLatch.countDown();
						}
				    });
				    
				    if(null == existStat){
				    	System.out.println("Path /" + uuid + " is not exist on ZK, need wait for the watcher");
				    	zkConnectLatch.await();
				    }else{
				    	throw new RuntimeException("Watcher test failed.");
				    }
				    
				    existStat = support.exists("/" + uuid, false);
				    
				    if(null != existStat){
				    	System.out.println("/" + uuid + " has exist on ZK.");
				    }else{
				    	throw new RuntimeException("Watcher test failed.");
				    }
				}catch(Exception e){
					e.printStackTrace();
				}
			}}).start();
	    
	    /**
	     * create one path to rigger that watcher.
	     */
	    new Thread(new Runnable(){

			@Override
			public void run() {
				try {
					Thread.sleep(1 * 1000);
					
					support.create("/" + uuid, null);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}}).start();
	}
}
