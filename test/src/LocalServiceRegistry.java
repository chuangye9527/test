package ctd.net.rpc.registry.support;


import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import ctd.net.rpc.desc.support.ProviderUrl;
import ctd.net.rpc.desc.support.ServiceDesc;
import ctd.net.rpc.registry.exception.RegistryException;

public class LocalServiceRegistry extends AbstractServiceRegistry {
	private static final Logger logger = LoggerFactory.getLogger(LocalServiceRegistry.class);
	private static final CopyOnWriteArraySet<ProviderUrl> lastConnectFailedUrls = new CopyOnWriteArraySet<>();

	private static final int CONNECT_RETRY_DELAY = 30;
	
	@Override
	protected LoadingCache<String, ServiceDesc> perpareServiceStore() {
		return CacheBuilder.newBuilder().build(new CacheLoader<String, ServiceDesc>(){
			@Override
			public ServiceDesc load(String key) throws Exception {
				throw new RegistryException("local registry not support load from loader.");
			}
			
		});
	}
	
	@Override
	public void deploy(ServiceDesc service) throws RegistryException {
		logger.info("local service[{}] deployed.",service.getId());
	}

	@Override
	public void undeployService(String beanName) throws RegistryException {
		throw new RegistryException("undeploy service disabled:" + beanName);
	}

	@Override
	public ServiceDesc find(String beanName) throws RegistryException {
		if(serviceStore.asMap().containsKey(beanName)){
			return serviceStore.getIfPresent(beanName);
		}
		return findLocalServiceBean(beanName);
	}

	@Override
	public void addLastConnectFailedUrl(final ProviderUrl url) {
		if(lastConnectFailedUrls.add(url)){
			schedule.schedule(new Runnable() {
				@Override
				public void run() {
					url.setLastConnectFailed(false);
					lastConnectFailedUrls.remove(url);
				}
			}, CONNECT_RETRY_DELAY, TimeUnit.SECONDS);
			
			
		}
	}

	

}
