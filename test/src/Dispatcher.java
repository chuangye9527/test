package ctd.net.rpc.server;

import ctd.net.rpc.Invocation;
import ctd.net.rpc.Result;
import ctd.net.rpc.beans.ServiceBean;
import ctd.net.rpc.desc.support.MethodDesc;
import ctd.net.rpc.registry.ServiceRegistry;
import ctd.spring.AppDomainContext;
import ctd.util.context.Context;
import ctd.util.context.ContextUtils;

public class Dispatcher {
	private static final ServiceRegistry registry = AppDomainContext.getRegistry();
	private static Dispatcher instance;
	private DispatcherFilter filter;
	
	public Dispatcher(){
		instance = this;
	}
	
	public static Dispatcher instance(){
		if(instance == null){
			instance = new Dispatcher();
		}
		return instance;
	}
	
	public Result invoke(Invocation invocation)  {
		
		Result result = null;
		try{
			
			String beanName = invocation.getBeanName();
			if(filter != null)
				filter.before(invocation);
			ServiceBean<?> service = registry.findLocalServiceBean(beanName);
			String methodDesc = invocation.getMethodDesc();
			Object[] parameters = invocation.getParameters();
			
			ContextUtils.put(Context.RPC_INVOKE_HEADERS, invocation.getAllHeaders());
			result = invoke(service,methodDesc,parameters);
			result.setPayloadType(invocation.getPayloadType());
			
		}
		catch(DispatcherFilterException e){
			result = new Result();
			result.setPayloadType(invocation.getPayloadType());
			if(e.getCause() != null){
				result.setException(e.getCause());
			}
			else{
				result.setException(e);
			}
		}
		catch(Throwable t){
			result = new Result();
			result.setPayloadType(invocation.getPayloadType());
			result.setException(t);
		}
		finally{
			try{
				if(filter != null)
					filter.after(result);
			}
			catch(DispatcherFilterException e){
				if(result.getException() == null){
					result.setValue(null);
					result.setException(e);
				};
			}
			ContextUtils.remove(Context.RPC_INVOKE_HEADERS);
		}
		result.setCorrelationId(invocation.getCorrelationId());
		return result;
	}
	
	public Result invoke(ServiceBean<?> service,String methodDesc, Object[] parameters){
		Result result = new Result();
		try{
			MethodDesc method = service.getMethodByDesc(methodDesc);
			result.setValue(method.invoke(service.getObject(), parameters));
			result.setCompression(method.getOutboundCompression());
			method.getOutboundCompression();
		}
		catch (Throwable t) {
			Throwable cause = t.getCause();
			if(cause != null){
				result.setException(cause);
			}
			else{
				result.setException(t);
			}
		}
		return result;
	}
	
	public void setFilter(DispatcherFilter filter){
		this.filter = filter;
	}

}
