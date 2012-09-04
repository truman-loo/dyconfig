/**
 * 
 */
package dyservice;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.KeeperException;

import com.cisco.dynconfig.zookeeper.ZookeeperWrapper;

/**
 * @author pelu2
 * @date Sep 1, 2012
 */
public class ZKStressTest {
	
	private AtomicInteger request = new AtomicInteger(0);
	private AtomicInteger readCount = new AtomicInteger(0);
	private AtomicInteger writeCount = new AtomicInteger(0);
	
	private byte[] data;
	
	private static final String executePath = "/test";
	
	public ZKStressTest(int size, int zkCount){
		generateData(size);
	}
	
	public static ZookeeperWrapper supports = ZookeeperWrapper.getInstance();
	
	AtomicInteger count = new AtomicInteger(0);
	
	private void generateData(int size){
		data = new byte[size];
		Random ran = new Random();
		ran.nextBytes(data);
	}
	
	public void doReadAction() {
		try{
			supports.getData(executePath, null, null);
			request.incrementAndGet();
			readCount.incrementAndGet();
		}catch(Exception e){
			// ignore this exception.
			e.printStackTrace();
		}
	}
	
	public void doExistTest(){
		try{
			supports.exists(executePath, false);
			request.incrementAndGet();
		}catch(Exception e){
			// ignore
			e.printStackTrace();
		}
	}
	
	public void doWriteAction() throws KeeperException, InterruptedException{
		try{
			supports.setData(executePath, data, -1);
			request.incrementAndGet();
			writeCount.incrementAndGet();
		}catch(Exception e){
			// ignore this exception.
			e.printStackTrace();
		}
	}
	
	public static void main(String... strings) throws KeeperException, InterruptedException {
		final int readRatio = 100;
		int size = 1 * 1024;
		Object lock = new Object();
		
		
		final ZKStressTest stress = new ZKStressTest(size, 1);
		supports.createEphemeralNode(executePath, null);

		for(int i = 0; i++ < 10; ){
			new DemonThread(new Runnable() {

				@Override
				public void run() {
					try {
						Random ran = new Random();
						
						while (true) {
//							stress.doExistTest();
							
							if(ran.nextInt(readRatio) == 1){
								stress.doWriteAction();
							}else{
								stress.doReadAction();
							}
							
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}).start();
		}
		
		long start = System.currentTimeMillis();
		System.out.println("Start timer.");
		synchronized(lock){
			try {
				lock.wait(1 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("End timer. " + (System.currentTimeMillis() - start));

		System.out.println("RPS: " + stress.request.get() + ", read: " + stress.readCount + ", write: " + stress.writeCount);
	}

	static class DemonThread extends Thread {

		public DemonThread(Runnable task) {
			super(task);
			super.setDaemon(true);
		}
	}
}
