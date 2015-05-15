package ctd.util.store.support;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ctd.util.store.StoreException;
import ctd.util.store.ActiveStore;
import ctd.util.store.listener.NodeListener;
import ctd.util.store.listener.StateListener;

public class ZooKeeperActiveStore implements ActiveStore, Watcher {
	private static final Logger logger = LoggerFactory.getLogger(ZooKeeperActiveStore.class);
	private static final int MAX_RETRYS = 3;
	private static final int SESSION_TIMEOUT = 10000;
	private final Lock connectingLock = new ReentrantLock();
	private final Lock reconnectingLock = new ReentrantLock();
	private final ExecutorService exec = Executors.newSingleThreadScheduledExecutor();
	private ZooKeeper zk;
	private String serverAddress;
	private Set<StateListener> stateListeners = new HashSet<StateListener>();
	
	private CountDownLatch connectLatch;
	
	public ZooKeeperActiveStore(String address){
		serverAddress = address;
	}
	
	@Override
	public void connect() {
		connectingLock.lock();
		try {
			if(isConnected()){
				return;
			}
			connectLatch = new CountDownLatch(1);
			zk = new ZooKeeper(serverAddress,SESSION_TIMEOUT,this);
			connectLatch.await();
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
		finally{
			connectingLock.unlock();
		}
	}
	
	@Override
	public void connectingAwait(){
		try {
			connectLatch.await();
		} 
		catch (InterruptedException e) {
			Thread.interrupted();
		}
	}
	
	@Override
	public boolean isConnected() {
		return (zk != null && zk.getState().isConnected());
	}

	@Override
	public void addStateListener(StateListener listener) {
		stateListeners.add(listener);
	}

	@Override
	public void delete(String path) throws StoreException {
		int retryCount = 0;
		while(true){
			try {
				Stat st = zk.exists(path, false);
				if(st != null){
					List<String> children = zk.getChildren(path, null);
					for(String c : children){
						delete(path + "/" + c);
					}
					zk.delete(path, st.getVersion());
				}
				return;
			}
			catch (KeeperException e) {
				processKeeperException(e,retryCount);
				retryCount ++;
			}
			catch (InterruptedException e) {
				Thread.interrupted();
			}
		}
	}

	@Override
	public boolean isPathExist(String path) throws StoreException {
		return isPathExist(path,null);
	}

	@Override
	public boolean isPathExist(final String path,final NodeListener listener) throws StoreException {
		int retryCount = 0;
		while(true){
			try {
				Watcher w = null;
				if(listener != null){
					w = new Watcher(){
						@Override
						public void process(WatchedEvent event) {
							ZooKeeperActiveStore.this.processKeeperWatchedEvent(event,listener);
						}
					};
				}
				Stat st = zk.exists(path, w);
				if(st == null){
					return false;
				}
				else{
					return true;
				}
			}
			catch (KeeperException e) {
				processKeeperException(e,retryCount);
				retryCount ++;
			}
			catch (InterruptedException e) {
				Thread.interrupted();
			}
		}
	}

	@Override
	public List<String> getChildren(String path) throws StoreException {
		return getChildren(path,null);
	}

	@Override
	public List<String> getChildren(final String path, final NodeListener listener)throws StoreException {
		int retryCount = 0;
		
		while(true){
			try {
				Watcher w = null;
				if(listener != null){
					w = new Watcher(){
						@Override
						public void process(WatchedEvent event) {
							ZooKeeperActiveStore.this.processKeeperWatchedEvent(event,listener);
						}
					};
				}
				return zk.getChildren(path, w);
			}
			catch (KeeperException e) {
				processKeeperException(e, retryCount);
				retryCount ++;
			}
			catch (InterruptedException e) {
				Thread.interrupted();
			}
		}
	}

	@Override
	public void setData(String path, byte[] data) throws StoreException {
		int retryCount = 0;
		while(true){
			try{
				Stat st = zk.exists(path, null);
				if(st != null){
					zk.setData(path, data, st.getVersion());
				}
				return;
			}
			catch(KeeperException e){
				processKeeperException(e, retryCount);
				retryCount++;
			}
			catch (InterruptedException e) {
				Thread.interrupted();
			}
		}
	}

	@Override
	public byte[] getData(String path) throws StoreException {
		return getData(path,null);
	}

	@Override
	public byte[] getData(final String path, final NodeListener listener) throws StoreException {
		int retryCount = 0;
		while(true){
			try{
				Stat st = zk.exists(path, null);
				if(st == null){
					throw new StoreException(StoreException.PATH_NOT_EXIST,"path not exist:" + path);
				}
				Watcher w = null;
				if(listener != null){
					w = new Watcher(){
						@Override
						public void process(WatchedEvent event) {
							ZooKeeperActiveStore.this.processKeeperWatchedEvent(event,listener);
						}
					};
				}
				return zk.getData(path, w, st);
			}
			catch(KeeperException e){
				processKeeperException(e, retryCount);
				retryCount ++;
			} 
			catch (InterruptedException e) {
				Thread.interrupted();
			}
		}
	}

	@Override
	public void createTempPath(String path, byte[] data) throws StoreException {
		create(path,data,CreateMode.EPHEMERAL);
	}

	@Override
	public String createSeqTempPath(String path, byte[] data) throws StoreException {
		return create(path,data,CreateMode.EPHEMERAL_SEQUENTIAL);
	}

	@Override
	public void createPath(String path, byte[] data) throws StoreException {
		create(path,data,CreateMode.PERSISTENT);
	}
	
	@Override
	public String createSeqPath(String path,byte[] data) throws StoreException{
		return create(path,data,CreateMode.PERSISTENT_SEQUENTIAL);
	}
	
	private String create(String path,byte[] data,CreateMode mode) throws StoreException{
		int retryCount = 0;
		while(true){
			try {
				return zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
			} 
			catch (KeeperException e){
				processKeeperException(e, retryCount);
				retryCount ++;
			}
			catch (InterruptedException e) {
				Thread.interrupted();
			}
		}
	}

	@Override
	public void process(WatchedEvent event) {
		onStateChange(event);
	}
	
	private void onStateChange(WatchedEvent event){
		KeeperState st = event.getState();
		switch(st){
			case SyncConnected:
				connectLatch.countDown();
				fireConnectedEvent();
				break;
			case Disconnected:
				fireDisconnectedEvent();
			case Expired:
				reconnect();
				break;
			default:
				break;		
		}
	}
	
	private void processKeeperWatchedEvent(final WatchedEvent event,final NodeListener listener){
		String path = event.getPath();
		if(path == null){
			exec.execute(new Runnable(){
				@Override
				public void run() {
					onStateChange(event);
				}//run
			});
			
			KeeperState st = event.getState();
			switch(st){
				case SyncConnected:
					listener.onConnected();
					break;
				case Disconnected:
					listener.onDisconnected();
				case Expired:
					listener.onExpired();
					break;
				default:
					break;		
			}
			
		}
		else{
			EventType type = event.getType();
			try{
				switch(type){
					case NodeDeleted:
						listener.onDeleted(path);
						break;
					case NodeCreated:
						listener.onCreated(path);
						break;
					case NodeDataChanged:
						listener.onDataChanged(path);
						break;
					case NodeChildrenChanged:
						listener.onChildrenChanged(path);
						break;
					case None:
					default:
						break;
				}
			}
			catch(Exception e){
				throw new IllegalStateException(e);
			}
		}//else
		
	}
	
	private void processKeeperException(KeeperException e,int retryCount) throws StoreException{
		
		if(retryCount > MAX_RETRYS){
			throw new StoreException(StoreException.RETRY_TIMES_EXCEED,e.getMessage());
		}
		else{
			if(e.code() == Code.SESSIONEXPIRED){
				reconnect();
				return;
			}
			if(e.code() == Code.CONNECTIONLOSS){
				fireDisconnectedEvent();
				return;
			}
			
			if(e.code() == Code.NONODE){
				throw new StoreException(StoreException.PATH_NOT_EXIST,e.getMessage());
			}
			if(e.code() == Code.NODEEXISTS){
				throw new StoreException(StoreException.PATH_EXIST,e.getMessage());
			}
			try {
				TimeUnit.SECONDS.sleep(3*retryCount);
			} 
			catch (InterruptedException e1) {
				Thread.currentThread().interrupt();
			}
		}
	}
	
	private void reconnect(){
		reconnectingLock.lock();
		try{
			if(isConnected()){
				return;
			}
			connect();
			fireExpiredEvent();
		}
		finally{
			reconnectingLock.unlock();
		}
	}
	
	private void fireConnectedEvent(){
		for(StateListener ls : stateListeners){
			try{
				ls.onConnected();
			}
			catch(Exception e){
				logger.error("fire connected event failed.",e);
			};
		}
	}
	
	private void fireDisconnectedEvent(){
		for(StateListener ls : stateListeners){
			try{
				ls.onDisconnected();
			}
			catch(Exception e){
				logger.error("fire disconnected event failed.",e);
			};
		}
	}
	
	private void fireExpiredEvent(){
		for(StateListener ls : stateListeners){
			try{
				ls.onExpired();
			}
			catch(Exception e){
				logger.error("fire expired event failed.",e);
			};
		}
	}

	@Override
	public String getServerAddress() {
		return serverAddress;
	}

	@Override
	public void close() {
		try {
			connectingLock.lockInterruptibly();
			zk.close();
			exec.shutdown();
		} 
		catch (InterruptedException e) {
			Thread.interrupted();
		}
		finally{
			stateListeners.clear();
			connectingLock.unlock();
		}
		
	}

}
