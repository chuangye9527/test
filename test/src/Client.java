package ctd.net.rpc;

import java.util.Map;

import ctd.net.rpc.balance.Balance;
import ctd.net.rpc.balance.BalanceFactory;
import ctd.net.rpc.desc.support.MethodDesc;
import ctd.net.rpc.desc.support.ProviderUrl;
import ctd.net.rpc.desc.support.ServiceDesc;
import ctd.net.rpc.exception.RemoteException;
import ctd.net.rpc.exception.RpcException;
import ctd.net.rpc.logger.InvokeLog;
import ctd.net.rpc.registry.ServiceRegistry;
import ctd.net.rpc.transport.exception.TransportException;
import ctd.net.rpc.transport.factory.TransportFactory;
import ctd.spring.AppDomainContext;
import ctd.util.context.Context;
import ctd.util.context.ContextUtils;

public class Client {
	private final static ServiceRegistry registry = AppDomainContext.getRegistry();
	
	private static Invocation createInvocation(String beanName,String methodName,Object[] parameters,Map<String,Object> headers,byte payloadType) throws RpcException{	
		if(registry == null){
			throw new RpcException(RpcException.REGISTRY_NOT_READY,"registry not ready or disable.");
		}
		ServiceDesc sc = registry.find(beanName);
		MethodDesc mc = null;
		if(payloadType == Payload.PAYLOAD_TYPE_NATIVE){
			mc = sc.getCompatibleMethod(methodName, parameters); 
		}
		else{
			mc = sc.getMethodByName(methodName);
		}
		if(mc == null){
			throw new RpcException(RpcException.METHOD_NOT_FOUND,"service[" + beanName + "] method[" + methodName + "] paramters is not compatiabled");
		}
		if(mc.parameterCount() == 0){
			parameters = null;
		}
		Invocation invocation = new Invocation();
		invocation.setBeanName(beanName);
		invocation.setMethodDesc(mc.desc());
		invocation.setCompression(mc.getInboundCompression());
		invocation.setTimeout(mc.getTimeout());
		invocation.setParameters(parameters);
		invocation.setHeaders(headers);
		invocation.setHeader(Context.FROM_DOMAIN, AppDomainContext.getName());
		return invocation;
	}
	
	@SuppressWarnings("unchecked")
	public static byte[] rpcInvoke(String beanName,String methodName,byte[] bytes) throws Exception{
		Object[] parameters = null;
		if(bytes != null){
			parameters = new Object[]{bytes};
		}
		
		Map<String,Object> headers = null;
		if(ContextUtils.hasKey(Context.RPC_INVOKE_HEADERS)){
			headers = (Map<String, Object>) ContextUtils.get(Context.RPC_INVOKE_HEADERS);
		}
		Invocation invocation = createInvocation(beanName,methodName,parameters,headers,Payload.PAYLOAD_TYPE_JSON);
		invocation.setPayloadType(Payload.PAYLOAD_TYPE_JSON);
		return (byte[]) rpcInvoke(invocation,null);
	}
	
	public static Object rpcInvoke(String beanName,String methodName,Object[] parameters,Map<String,Object> headers,Balance balance) throws Exception{
		Invocation invocation = createInvocation(beanName,methodName,parameters,headers,Payload.PAYLOAD_TYPE_NATIVE);
		return rpcInvoke(invocation,balance);
	}
	
	private static Object rpcInvoke(Invocation invocation,Balance balance) throws Exception{
		InvokeLog log = new InvokeLog();
		try{
			log.begin();
			log.setInvocation(invocation);
			String beanName = invocation.getBeanName();
			ServiceDesc sc = registry.find(beanName);
			if(balance == null){
				balance = BalanceFactory.getBalance(sc.getProperty("balance", String.class)); 
			}
			
			int maxRetrys = sc.providerUrlsCount();
			int retryCount = 0;
			while(true){
				ProviderUrl url = balance.select(sc.providerUrls());
				if(url == null || maxRetrys == 0){
					throw new RpcException(RpcException.SERVICE_OFFLINE,"service[" + beanName + "] is not available.");
				}
				log.setRetryCount(retryCount);
				log.setUrl(url);
				try{
					ctd.net.rpc.transport.Client client = TransportFactory.createClient(url.getUrl());
					if(client == null){
						throw new RpcException(RpcException.INVAILD_URL,"service[" + beanName + "]@url[" + url.getUrl() + "] is invaild.");
					}
					Result result = client.invoke(invocation);
					log.setResult(result);
					result.throwExpceptionIfHas();
					return result.getValue();
				}
				catch(TransportException e){
					if(e.isConnectFailed()){
						url.setLastConnectFailed(true);
					}
					else if(e.isTimeout()){
						url.setLastTimeout(invocation.getTimeout() * 1000);
					}
					else{
						throw e;
					}
					retryCount ++;
					if(retryCount <= maxRetrys){
						continue;
					}
					throw e;
				}
			} // while
			
		}
		catch(RemoteException e){
			throw (Exception)e.getCause();
		}
		catch(RpcException e){
			log.setRpcException(e);
			throw e;
		}
		finally{
			log.finish();
		}
	}
	
	public static Object rpcInvoke(String beanName,String methodName,Object[] parameters,Map<String,Object> headers) throws Exception{
		return rpcInvoke(beanName,methodName,parameters,headers,null);
	}
	
	@SuppressWarnings("unchecked")
	public static Object rpcInvoke(String beanName,String methodName,Object ...parameters) throws Exception{
		Map<String,Object> headers = null;
		if(ContextUtils.hasKey(Context.RPC_INVOKE_HEADERS)){
			headers = (Map<String, Object>) ContextUtils.get(Context.RPC_INVOKE_HEADERS);
		}
		return rpcInvoke(beanName,methodName,parameters,headers);
	}
	
	@SuppressWarnings("unchecked")
	public static Object rpcInvoke(String beanName,String methodName,Balance balance,Object ...parameters) throws Exception{
		Map<String,Object> headers = null;
		if(ContextUtils.hasKey(Context.RPC_INVOKE_HEADERS)){
			headers = (Map<String, Object>) ContextUtils.get(Context.RPC_INVOKE_HEADERS);
		}
		return rpcInvoke(beanName,methodName,parameters,headers,balance);
	}
	
	@SuppressWarnings("unchecked")
	public static Object rpcInvoke(String beanName,String methodName) throws Exception{
		Map<String,Object> headers = null;
		if(ContextUtils.hasKey(Context.RPC_INVOKE_HEADERS)){
			headers = ContextUtils.get(Context.RPC_INVOKE_HEADERS,Map.class);
		}
		return rpcInvoke(beanName,methodName,null,headers,null);
	}
}
