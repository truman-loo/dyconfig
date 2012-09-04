/**
 * 
 */
package dyservice;

import org.junit.Test;

import dyservice.zookeeper.ZookeeperWrapper;


/**
 * @author pelu2
 * @date Sep 1, 2012
 */
public abstract class ZookeeperBaseTest {
	public static ZookeeperWrapper support = ZookeeperWrapper.getInstance();

	public static final String charsetName = "utf-8";
	
	@Test
	public abstract void testWatcher();
}
