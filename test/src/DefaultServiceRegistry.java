package ctd.net.rpc.registry.support;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import ctd.net.rpc.desc.support.ProviderUrl;
import ctd.net.rpc.desc.support.ServiceDesc;
import ctd.net.rpc.exception.RpcException;
import ctd.net.rpc.registry.exception.RegistryException;
import ctd.net.rpc.transport.ServerUrl;
import ctd.spring.AppDomainContext;
import ctd.util.acl.ACListType;
import ctd.util.store.ActiveStore;
import ctd.util.store.StoreConstants;
import ctd.util.store.StoreException;
import ctd.util.store.listener.NodeListener;
import ctd.util.store.listener.StateListener;

public class DefaultServiceRegistry extends AbstractServiceRegistry implements StateListener,Runnable {
	private static final Logger logger = LoggerFactory.getLogger(DefaultServiceRegistry.class);
	private static final int PUBLISH_CHECK_DELAY = 5;
	private static final int RETRY_DELAY = 5;
	private static final int REMOVE_SERVICE_EXPIRE_MINUTES = 60;
	private static final int STATIC_URL_CONNECT_RETRY_DELAY = 30;
	private static final String CHARSET = "UTF-8";
	private static final ConcurrentLinkedQueue<ServiceDesc> uploadQueue = new ConcurrentLinkedQueue<ServiceDesc>();
	private static final CopyOnWriteArraySet<ProviderUrl> lastConnectFailedUrls = new CopyOnWriteArraySet<>();
	
	
	private final ConcurrentHashMap<String, ProviderUrlListener> providerUrlListeners = new ConcurrentHashMap<>();
	private final CopyOnWriteArraySet<ServiceDesc> deployedSet = new CopyOnWriteArraySet<ServiceDesc>();
	
	private Thread t;
	private ActiveStore store;
	private String domainServiceRoot;
	
	protected LoadingCache<String, ServiceDesc> perpareServiceStore(){
		return CacheBuilder.newBuilder()
				.expireAfterAccess(REMOVE_SERVICE_EXPIRE_MINUTES, TimeUnit.MINUTES)
				.removalListener(new RemovalListener<String,ServiceDesc>() {
					@Override
					public void onRemoval(RemovalNotification<String, ServiceDesc> notification) {
						logger.info("service[{}] expired,removed from serviceStore.",notification.getKey());
					}
				})
				.build(new CacheLoader<String,ServiceDesc>(){
					@Override
					public ServiceDesc load(String beanName) throws Exception {
						return loadFromStore(beanName);
					}
				});
	}
	
	
	private ServiceDesc loadFromStore(String beanName) throws RegistryException{
		String domain = StringUtils.substringBefore(beanName, ".");
		String path = buildPathStr(StoreConstants.SERVICES_HOME , "/" , domain , "/" , beanName);
		try {
			byte[] data = store.getData(path);
			ServiceDesc s = ServiceDesc.parse(new String(data,CHARSET));
			String providerUrlsRoot = buildPathStr(path , "/" , StoreConstants.SERVICE_PROVIDERS);
			
			List<String> ls = store.getChildren(providerUrlsRoot);
			updateServiceProvoders(s,ls);
			
			startProviderUrlsWatch(beanName,providerUrlsRoot);
			startServiceRegistryWatch(beanName,path);
			return s;
		} 
		catch (StoreException e) {
			if(e.isPathNotExist()){
				throw new RegistryException(RpcException.SERVICE_NOT_REGISTED, "beanName[" + beanName + "] not found on server registry"); 
			}
			throw new RegistryException(RpcException.UNKNOWN, "beanName[" + beanName + "] load from registry failed.",e);
		} 
		catch (UnsupportedEncodingException e) {
			throw new RegistryException(RpcException.UNKNOWN, "beanName[" + beanName + "] load from registry failed.",e);
		}
			
	}
	
	@Override
	public void deploy(ServiceDesc service) throws RegistryException{
		uploadQueue.add(service);
	}
	

	@Override
	public void addLastConnectFailedUrl(final ProviderUrl url){
		if(url.isStatic()){
			schedule.schedule(new Runnable() {
				@Override
				public void run() {
					url.setLastConnectFailed(false);
					lastConnectFailedUrls.remove(url);
				}
			}, STATIC_URL_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);
		}
		else{
			lastConnectFailedUrls.add(url);
		}
	}
	
	@Override
	public ServiceDesc find(String beanName) throws RegistryException{
		store.connectingAwait();
		try {
			return serviceStore.get(beanName);
		} 
		catch (ExecutionException e) {
			Throwable t = e.getCause();
			if(t instanceof RegistryException){
				throw (RegistryException)t;
			}
			throw new RegistryException(t);
		}
		
	}

	
	public  void startServiceRegistryWatch(final String beanName,final String path){
	
		try{
			store.isPathExist(path, new NodeListener(){
				@Override
				public void onDeleted(String path){
					serviceStore.invalidate(beanName);
					logger.info("service[" + beanName + "] unregistered.");
				}
				
				@Override
				public void onDataChanged(String path){
					serviceStore.invalidate(beanName);
					logger.info("service[" + beanName + "] updated,clear local cache.");
				}
				
			});
			
		}
		catch(StoreException e){
			logger.error(e.getMessage(),e);
			try {
				TimeUnit.SECONDS.sleep(RETRY_DELAY);
			} 
			catch (InterruptedException e1) {
				Thread.currentThread().interrupt();
			}
			startServiceRegistryWatch(beanName,path);
		}
		
	}
	
	private void startProviderUrlsWatch(final String beanName,final String path){
		startProviderUrlsWatch(beanName,path,false);
	}
	
	private void updateServiceProvoders(ServiceDesc service,List<String> ls){
		if(ls == null){
			return;
		}
		List<ProviderUrl> urls = new ArrayList<ProviderUrl>();
		for(String s : ls){
			try {
				String urlPath = URLDecoder.decode(s, CHARSET);
				ProviderUrl url = new ProviderUrl(urlPath);
				url.setBeanName(service.getId());
				urls.add(url);
			}
			catch (UnsupportedEncodingException e) {}
		}
		service.updateProviderUrls(urls);
	}
	
	private void startProviderUrlsWatch(final String beanName,final String path,boolean justWactchEvent){
		try{
			ProviderUrlListener lis = new ProviderUrlListener(){
				@Override
				public void onChildrenChanged(String path){
					if(!isDisabled() && serviceStore.asMap().containsKey(beanName)){
						startProviderUrlsWatch(beanName,path);
					}
				}
				
				@Override
				public void onConnected(){//reconnected
					if(!isDisabled() && serviceStore.asMap().containsKey(beanName)){
						startProviderUrlsWatch(beanName,path);
					}
				}
			};
			
			List<String> ls = store.getChildren(path, lis);
			ProviderUrlListener old = providerUrlListeners.put(path, lis);
			if(old != null){
				old.setDisabled(true);
			}
			if(justWactchEvent){
				return;
			}
			ServiceDesc service = serviceStore.getIfPresent(beanName);
			if(service != null && ls != null){
				updateServiceProvoders(service,ls);
			}
		}
		catch(StoreException e){
			logger.error(e.getMessage(),e);
			try {
				TimeUnit.SECONDS.sleep(RETRY_DELAY);
			} 
			catch (InterruptedException e1) {
			}
			startProviderUrlsWatch(beanName,path);
		}
	}
	
	@Override
	public void undeployService(String beanName) throws RegistryException{
		int i = beanName.indexOf(".");
		String domain = beanName.substring(0,i);
		String domainServiceRoot = buildPathStr(StoreConstants.SERVICES_HOME , "/" ,domain);
		String servicePath = buildPathStr(domainServiceRoot , "/" , beanName) ;
		try {
			store.delete(servicePath);
		} 
		catch (StoreException e) {
			throw new RegistryException(e);
		}
	}
	
	public void deployService(String beanName,String serviceDesc,boolean overwrite) throws RegistryException{
		int i = beanName.indexOf(".");
		String domain = beanName.substring(0,i);
		deployService(domain,beanName,serviceDesc,overwrite);
	}
	
	public void deployService(String domain,String beanName,String serviceDesc,boolean overwrite) throws RegistryException {
		String domainServiceRoot = buildPathStr(StoreConstants.SERVICES_HOME , "/" , domain);
		String servicePath = buildPathStr(domainServiceRoot , "/" , beanName) ;
	
		try {
			if(!store.isPathExist(domainServiceRoot)){
				store.createPath(domainServiceRoot,null);
			}
			byte[] serviceData = serviceDesc.getBytes(CHARSET);
			if(!store.isPathExist(servicePath)){
				store.createPath(servicePath, serviceData);
				store.createPath(buildPathStr(servicePath , "/" , StoreConstants.SERVICE_ACL), null);
				store.createPath(buildPathStr(servicePath , "/" , StoreConstants.SERVICE_PROVIDERS), null);
				logger.info("service[" + beanName + "] path created.");
			}
			else{

				byte[] data = store.getData(servicePath);
				String serverSideDesc = new String(data,CHARSET);
				
				ServiceDesc L = ServiceDesc.parse(serviceDesc);
				ServiceDesc S = ServiceDesc.parse(serverSideDesc);
				
				if(!L.equals(S)){
					if(overwrite){
						store.setData(servicePath, serviceData);
						logger.info("service[" + beanName + "] overwrited.");
					}
					else{
						throw new IllegalStateException("service[" + beanName + "] is not compatible with the registry one,deploy failed.");
					}
				}

			}
		} 
		catch(StoreException e){
			throw new RegistryException(e);
		}
		catch (UnsupportedEncodingException e) {
			throw new RegistryException(e);
		}
	}
	
	public void deployProviderUrl(ServiceDesc service) throws RegistryException{
		String domain = service.getAppDomain();
		if(StringUtils.isEmpty(domain)){
			domain = AppDomainContext.getName();
		}
		String domainServiceRoot = buildPathStr(StoreConstants.SERVICES_HOME , "/" , domain);
		
		String beanName = service.getId();	
		String providerUrlsRoot = buildPathStr(domainServiceRoot,"/",beanName,"/",StoreConstants.SERVICE_PROVIDERS);
		
		try{
			if(!store.isPathExist(providerUrlsRoot)){
				store.createPath(providerUrlsRoot,null);
			}
			
			for(ServerUrl url : serverUrls){
				
				String providerUrlPath = buildPathStr(providerUrlsRoot , "/" , url.getEncodeUrl());
				if(store.isPathExist(providerUrlPath)){
					store.delete(providerUrlPath);
				}
				store.createTempPath(providerUrlPath, null);
			}
			deployedSet.add(service);
			startWatchACL(service);
		}
		catch(StoreException e){
			throw new RegistryException(e);
		}
		
		logger.info("service[" + beanName + "] online.");
	}
	
	public void undeployProviderUrl(ServiceDesc service) throws StoreException{
		String domain = service.getAppDomain();
		if(StringUtils.isEmpty(domain)){
			domain = AppDomainContext.getName();
		}
		String domainServiceRoot = buildPathStr(StoreConstants.SERVICES_HOME , "/" , domain);
		
		String beanName = service.getId();
		for(ServerUrl url : serverUrls){
			String providerUrlPath = buildPathStr(domainServiceRoot,"/",beanName,"/",StoreConstants.SERVICE_PROVIDERS,"/",url.getUrl());
			store.delete(providerUrlPath);
			deployedSet.remove(service);
		}
		
		logger.info("service[" + beanName + "] offline");
	}
	
	public void addServiceACLItem(String beanName,String aclStr) throws StoreException{
		String domain = StringUtils.substringBefore(beanName, ".");
		String ACLPath = buildPathStr(StoreConstants.SERVICES_HOME , "/" , domain , "/" , beanName , "/" , StoreConstants.SERVICE_ACL);
		if(!store.isPathExist(ACLPath)){
			store.createPath(ACLPath, null);
		}
		String ACLItemPath = buildPathStr(ACLPath , "/" , aclStr);
		if(!store.isPathExist(ACLItemPath)){
			store.createPath(ACLItemPath, null);
		}
	}
	
	public void removeServiceACLItem(String beanName,String aclStr) throws StoreException{
		String domain = StringUtils.substringBefore(beanName, ".");
		String ACLPath = buildPathStr(StoreConstants.SERVICES_HOME , "/" , domain , "/" , beanName , "/" , StoreConstants.SERVICE_ACL);
		if(!store.isPathExist(ACLPath)){
			return;
		}
		String ACLItemPath = buildPathStr(ACLPath , "/" , aclStr);
		if(store.isPathExist(ACLItemPath)){
			store.delete(ACLItemPath);
		}
	}
	
	public List<String> getServiceACL(String beanName) throws StoreException{
		int i = beanName.indexOf(".");
		String domain = beanName.substring(0,i);
		String ACLPath = buildPathStr(StoreConstants.SERVICES_HOME , "/" , domain , "/" , beanName , "/" , StoreConstants.SERVICE_ACL);
		if(!store.isPathExist(ACLPath)){
			return null;
		}
		return store.getChildren(ACLPath);
	}
	
	private void startWatchACL(final ServiceDesc service){
		String beanName = service.getId();
		
		if(!deployedSet.contains(service)){
			logger.info("service[" + beanName + "] stopping watch ACL as be offlined.");
			return;
		}
		
		String path = buildPathStr(domainServiceRoot, "/" , beanName , "/" , StoreConstants.SERVICE_ACL);
		try{
			List<String> ls = store.getChildren(path, new NodeListener(){
				@Override
				public void onChildrenChanged(String path){
					startWatchACL(service);
				}
			});
			service.updateACL(ACListType.whiteList, ls);
		}
		catch(StoreException e){
			logger.error(e.getMessage(),e);
			try {
				TimeUnit.SECONDS.sleep(RETRY_DELAY);
			} 
			catch (InterruptedException e1) {
			}
			startWatchACL(service);
		}
	}
	
	private void deployDomainServerNode() throws StoreException{
		String domain = AppDomainContext.getName();
		String serverNodesHome = StoreConstants.SERVERNODES_HOME;
		
		if(!store.isPathExist(serverNodesHome)){
			store.createPath(serverNodesHome, null);
		}
		
		String domainPath = buildPathStr(serverNodesHome , "/" , domain);
		if(!store.isPathExist(domainPath)){
			store.createPath(domainPath, null);
		}

		for(ServerUrl url : serverUrls){
			String serverHost = buildPathStr(url.getHost(),":",String.valueOf(url.getPort()));
			String serverNodePath = buildPathStr(domainPath , "/" ,serverHost,"-");
			if(store.isPathExist(serverNodePath)){
				store.delete(serverNodePath);
			}
			store.createSeqTempPath(serverNodePath, null);
			logger.info("serverNode[" + serverHost + "] deployed for domain[" + domain + "]");
		}
	}
	
	
	
	public void start(){
		if(t == null || t.isAlive()){
			running = true;
			t = new Thread(this,"ssdev-rpc-registry");
			//t.setDaemon(true);
			t.setPriority(Thread.MAX_PRIORITY);
			t.start();
		}
	}
	
	@Override
	public void shutdown(){
		super.shutdown();
		t.interrupt();
	}
	
	public  void setStore(ActiveStore store){
		this.store = store;
		store.addStateListener(this);
	}
	
	public ActiveStore getStore(){
		return store;
	}
	
	@Override
	public void onConnected(){
		
	}
	
	@Override
	public void onExpired(){
		try {
			serviceStore.invalidateAll();
			deployDomainServerNode();
		} 
		catch (StoreException e) {
			logger.info("redeploy domainServerNode falied:" + e.getMessage());
		}
		for(ServiceDesc service : deployedSet){
			try {
				deploy(service);
			} 
			catch (RegistryException e) {
				logger.error("redeploy failed:",e);
			}
		}
		logger.info("store is rebuilded,redeploy local services.");
	}

	@Override
	public void run() {
		try{
			prepareStore();
			deployDomainServerNode();
			while(running && !Thread.currentThread().isInterrupted()){
				try{
					deployLocalServices();
					checkLastFailedURls();
					TimeUnit.SECONDS.sleep(PUBLISH_CHECK_DELAY);
				}
				catch(StoreException e){
					logger.error("ServiceRegistry thread error.",e);
				}
				catch(InterruptedException e){
					Thread.currentThread().interrupt();
				}
				
			}
		}
		catch(Exception e){
			logger.error("Registry main thread shutdown now because has exception:" + e.getMessage());
		}
		logger.info("ServiceRegistry shutdown.");
	}
	
	
	
	private void checkLastFailedURls() throws StoreException {
		
		for(ProviderUrl url : lastConnectFailedUrls){
			String beanName = url.getBeanName();
			ServiceDesc service = serviceStore.getIfPresent(beanName); 
			if(service == null){
				lastConnectFailedUrls.remove(url);
				continue;
			}
			String domain = service.getAppDomain();		
			String providerUrlPath = buildPathStr(StoreConstants.SERVICES_HOME , "/" , domain,"/",beanName,"/",StoreConstants.SERVICE_PROVIDERS,"/" , url.getEncodeUrl());
			if(store.isPathExist(providerUrlPath)){
				url.setLastConnectFailed(false);
			}
			else{
				service.removeProviderUrl(url);
			}
			lastConnectFailedUrls.remove(url);
		}
		
	}

	private void deployLocalServices() throws RegistryException{
		
		ServiceDesc service = null;
		String domain = AppDomainContext.getName();
		while((service = uploadQueue.poll()) != null){
			if(serverUrls.isEmpty()){
				throw new RegistryException("serverUrls is empty,deploy abort.");
			}
			String beanName = service.getId();
			String serviceDesc = service.desc();
			boolean overwrite = true;//service.getProperty("master", boolean.class,false);

			deployService(domain,beanName,serviceDesc,overwrite);
			deployProviderUrl(service);
			
		}
	}
	
	
	private void prepareStore() throws StoreException{
		ActiveStore store = getStore();
		
		logger.info("registry thread start, preparing to connect server.");
		if(store == null){
			throw new IllegalStateException("ActiveStore is not inited.registry start failed.");
		}
		store.connect();
		
		if(!store.isPathExist(StoreConstants.ROOT_DIR)){
			store.createPath(StoreConstants.ROOT_DIR, null);
		}
		if(!store.isPathExist(StoreConstants.SERVICES_HOME)){
			store.createPath(StoreConstants.SERVICES_HOME, null);
		}
		String domain = AppDomainContext.getName();
		domainServiceRoot = buildPathStr(StoreConstants.SERVICES_HOME , "/" ,domain);

		if(!store.isPathExist(domainServiceRoot)){
			store.createPath(domainServiceRoot,null);
		}
		
		logger.info("registry is up,connected to server[" + store.getServerAddress() + "]");
	}
	
	private String buildPathStr(String ...strings){
		StringBuilder sb = new StringBuilder();
		for(String s : strings){
			sb.append(s);
		}
		return sb.toString();
	}

	@Override
	public void onDisconnected() {
		
	}
}
