package ctd.net.rpc.desc.support;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ctd.net.rpc.desc.AbstractDescription;
import ctd.util.ReflectUtil;
import ctd.util.annotation.RpcService;

public class MethodDesc extends AbstractDescription {
	private static final long serialVersionUID = 4449667363498057868L;

	private int index;
	private String name;
	private ParameterDesc returnType;
	private List<ParameterDesc> parameters = new ArrayList<ParameterDesc>();
	private String desc;
	private Method method;
	private byte inboundCompression;
	private byte outboundCompression;
	private int timeout = 20;

	public MethodDesc() {
	}

	public MethodDesc(Class<?> clz, Method m) {
		name = m.getName();
		method = m;
		method.setAccessible(true);

		Class<?> retClz = m.getReturnType();
		setReturnType(new ParameterDesc(retClz));

		String[] paramNames = ReflectUtil.getMethodParameterNames(m);
		Type[] types = m.getGenericParameterTypes();

		int i = 0;
		for (Type type : types) {
			ParameterDesc p = null;

			Class<?> typeClz = null;
			Type tp = null;

			if (type instanceof ParameterizedType) {
				ParameterizedType pt = (ParameterizedType) type;
				typeClz = (Class<?>) pt.getRawType();
				if (List.class.isAssignableFrom(typeClz)) {
					tp = pt.getActualTypeArguments()[0];
				} else if (Map.class.isAssignableFrom(typeClz)) {
					tp = pt.getActualTypeArguments()[1];
				}
			} else {
				if (type instanceof TypeVariable) {
					Type t = ReflectUtil.findTypeVariableParameterizedType(
							(TypeVariable<?>) type, clz);
					if (t instanceof ParameterizedType) {
						ParameterizedType pt = (ParameterizedType) t;
						typeClz = (Class<?>) pt.getRawType();
						if (List.class.isAssignableFrom(typeClz)) {
							tp = pt.getActualTypeArguments()[0];
						} else if (Map.class.isAssignableFrom(typeClz)) {
							tp = pt.getActualTypeArguments()[1];
						}
					} else {
						typeClz = (Class<?>) t;
					}
				} else {
					typeClz = (Class<?>) type;
				}

			}
			p = new ParameterDesc(typeClz, tp);
			if (paramNames != null) {
				p.setId(paramNames[i]);
			}
			addParameter(p);
			i++;
		}

	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public ParameterDesc getReturnType() {
		if (returnType == null) {
			returnType = new ParameterDesc();
			returnType.setTypeClassName("void");
		}
		return returnType;
	}

	public void setReturnType(ParameterDesc returnType) {
		this.returnType = returnType;
	}

	public List<ParameterDesc> getParameters() {
		return parameters;
	}

	public ParameterDesc getParameterAt(int i) {
		if (i >= 0 && i < parameters.size()) {
			return parameters.get(i);
		} else {
			throw new IllegalArgumentException("index[" + i + "] overflow.");
		}
	}

	public void setParameters(List<ParameterDesc> parameters) {
		this.parameters = parameters;
	}

	public void addParameter(ParameterDesc parameter) {
		parameters.add(parameter);
	}

	public int parameterCount() {
		return parameters.size();
	}

	public Class<?>[] parameterTypes() {
		int n = parameters.size();

		Class<?>[] types = new Class<?>[n];

		int i = 0;
		for (ParameterDesc p : parameters) {
			types[i] = p.typeClass();
			i++;
		}
		return types;
	}

	public String[] parameterNames() {
		int n = parameters.size();
		if (n == 0) {
			return null;
		}
		String[] typeNames = new String[n];

		int i = 0;
		for (ParameterDesc p : parameters) {
			typeNames[i] = p.getTypeClassName();
			i++;
		}
		return typeNames;
	}

	public boolean isCompatible(Object[] args) {
		boolean result = true;
		if (args == null && parameters.size() == 0) {
			return result;
		}
		int i = 0;
		for (ParameterDesc p : parameters) {
			Object o = args[i];
			if (!p.isCompatible(o)) {
				result = false;
				break;
			}
			i++;
		}
		return result;
	}

	public Object invoke(Object bean, Object[] parameters) throws Exception {
		return method.invoke(bean, parameters);
	}

	public String desc() {
		if (desc != null) {
			return desc;
		} else {
			int n = parameters.size();
			StringBuilder sb = new StringBuilder(name);
			sb.append("(");
			for (int i = 0; i < n; i++) {
				if (i > 0)
					sb.append(",");
				sb.append(parameters.get(i).getTypeClassName());
			}
			desc = sb.append(")").toString();
		}
		return desc;
	}

	public byte getInboundCompression() {
		return inboundCompression;
	}

	public void setInboundCompression(byte inboundCompression) {
		this.inboundCompression = inboundCompression;
	}

	public byte getOutboundCompression() {
		return outboundCompression;
	}

	public void setOutboundCompression(byte outboundCompression) {
		this.outboundCompression = outboundCompression;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}
}
