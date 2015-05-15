package ctd.net.rpc.json.parser;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;


import org.apache.commons.lang3.StringUtils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import ctd.net.rpc.desc.support.MethodDesc;
import ctd.net.rpc.desc.support.ParameterDesc;
import ctd.net.rpc.desc.support.ServiceDesc;
import ctd.net.rpc.json.JSONRequestBean;
import ctd.net.rpc.util.ServiceAdapter;
import ctd.spring.AppDomainContext;
import ctd.util.ReflectUtil;
import ctd.util.converter.ConversionUtils;

public class JSONRequestParser {
	private static final String NM_SERVICE_ID = "serviceId";
	private static final String NM_BEAN_NAME  = "beanName";
	private static final String NM_METHOD     = "method"; 
	private static final String NM_BODY       = "body";
	private static final String NM_PARAMETERS = "parameters";
	
	private static final String LOCAL_DOMAIN_PLACE_HOLDER = "$.";
	private static final LoadingCache<Class<?>, BeanInfo> beans =  CacheBuilder.newBuilder().build(new CacheLoader<Class<?>,BeanInfo>(){
		@Override
		public BeanInfo load(Class<?> key) throws Exception {
			return new BeanInfo(key);
		}
		
	});
	
	private static final JsonFactory jf = new JsonFactory(); 
	static{
		jf.enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
		jf.enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES);
		jf.enable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
	}
	
	public static void warmUpBean(Class<?> clz){
		try{
			beans.get(clz);
		}
		catch(Exception e){
			
		}
	}
	
	public static JSONRequestBean parse(byte[] bytes) {
		try{
			JsonParser jp = jf.createParser(bytes);
			return parse(jp);
		}
		catch(JsonParseException e){
			throw new JSONRequestParseException("json parse error.",e);
		} 
		catch (IOException e) {
			throw new JSONRequestParseException("json parse io error.",e);
		}
	}
	
	public static JSONRequestBean parse(String s) {
		try{
			JsonParser jp = jf.createParser(s);
			return parse(jp);
		}
		catch(JsonParseException e){
			throw new JSONRequestParseException("json parse error.",e);
		} 
		catch (IOException e) {
			throw new JSONRequestParseException("json parse io error.",e);
		}
	}
	
	public static JSONRequestBean parse(InputStream is) {
		try{
			JsonParser jp = jf.createParser(is);
			return parse(jp);
		}
		catch(JsonParseException e){
			throw new JSONRequestParseException("json parse error.",e);
		} 
		catch (IOException e) {
			throw new JSONRequestParseException("json parse io error.",e);
		}
	}
	
	public static Object[] parseParameters(MethodDesc methodDesc,byte[] bytes){
		try{
			JsonParser jp = jf.createParser(bytes);
			return parseBody(methodDesc,jp);
		}
		catch(JsonParseException e){
			throw new JSONRequestParseException("json parse error.",e);
		} 
		catch (IOException e) {
			throw new JSONRequestParseException("json parse io error.",e);
		}
	}
	
	private static JSONRequestBean parse(JsonParser jp){
		try{
			if (jp.nextToken() != JsonToken.START_OBJECT) {
				throw new JSONRequestParseException(JSONRequestParseException.JSON_OBJECT_NEEDED,"Expected data to start with an Object");
			}
			
			String serviceId = null;
			String method = null;
			Object[] parameters = null;
			Map<String,Object> properties = null;
			
			while(jp.nextToken() != null){
				JsonToken token = jp.getCurrentToken();
				if(token == JsonToken.FIELD_NAME){
					String nm = jp.getCurrentName();
					switch(nm){
						case NM_SERVICE_ID:
						case NM_BEAN_NAME:
							serviceId = jp.nextTextValue();
							if(StringUtils.isEmpty(serviceId)){
								throw new JSONRequestParseException(JSONRequestParseException.SERVICE_ID_MISSING,"serviceId is required.");
							}
							if(serviceId.startsWith(LOCAL_DOMAIN_PLACE_HOLDER)){
								serviceId = StringUtils.replaceOnce(serviceId, "$", AppDomainContext.getName());
							}
							break;
							
						case NM_METHOD:
							method = jp.nextTextValue();
							if(StringUtils.isEmpty(method)){
								throw new JSONRequestParseException(JSONRequestParseException.METHOD_MISSING,"method is required.");
							}
							break;
						
						
						case NM_BODY:
						case NM_PARAMETERS:
							if(StringUtils.isEmpty(serviceId) || StringUtils.isEmpty(method)){
								throw new JSONRequestParseException(JSONRequestParseException.BODY_POSITION,"[serviceId] and [method] must defined before [body].");
							}
							ServiceDesc serviceDesc = ServiceAdapter.getServiceDesc(serviceId);
							if(serviceDesc == null){
								throw new JSONRequestParseException(JSONRequestParseException.SERVICE_NOT_FOUND,"service[" + serviceId + "] not found.");
							}
							MethodDesc methodDesc = serviceDesc.getMethodByName(method);
							if(methodDesc == null){
								throw new JSONRequestParseException(JSONRequestParseException.METHOD_NOT_FOUND,"service[" + serviceId + "] method[" + method + "] not found.");
							}
							parameters = parseBody(methodDesc,jp);
							break;
						
						default:
							if(properties == null){
								properties = new HashMap<>();
							}
							jp.nextToken();
							properties.put(nm, parseValue(Object.class, null, jp));
					}
				} //if
			}//while
			if(StringUtils.isEmpty(serviceId) || StringUtils.isEmpty(method)){
				throw new JSONRequestParseException(JSONRequestParseException.BODY_POSITION,"[serviceId] and [method] is required.");
			}
			return new JSONRequestBean(serviceId,method,parameters,properties);
		}
		catch(IOException e){
			throw new JSONRequestParseException(JSONRequestParseException.IO_ERROR,e);
		}
	}

	private static Object[] parseBody(MethodDesc methodDesc, JsonParser jp)  {
			try{
				if(jp.nextToken() != JsonToken.START_ARRAY){
					throw new JSONRequestParseException(JSONRequestParseException.JSON_OBJECT_NEEDED,"Expected [body] to start with an Array");
				}
				
				List<ParameterDesc> params =  methodDesc.getParameters();
				int n = params.size();
				if(n == 0){
					return null;
				}
				Object[] result = new Object[params.size()];
				int i = 0;
				for(ParameterDesc p : params){
					Class<?> type = p.typeClass();
					jp.nextToken();
					result[i] = parseValue(type,p.actualTypeClass(),jp,true);
					i++;
				}
				return result;
			}
			catch(JsonParseException e){
				throw new JSONRequestParseException("json parse error.",e);
			}
			catch(IOException e){
				throw new JSONRequestParseException("json parse io error.",e);
			}
		
	}
	
	private static Object parseValue(Class<?> typeClass,Type type,JsonParser jp) {
		return parseValue(typeClass,type,jp,false);
	}
	
	private static Object parseValue(Class<?> typeClass,Type type,JsonParser jp,boolean root) {
		try{
			JsonToken token = jp.getCurrentToken();
			if(token == null){
				throw new JSONRequestParseException(500,"unexpected end.");
			}
			
			switch(token){
				case VALUE_NULL:
					return null;
			
				case START_ARRAY:
					if(typeClass == Object.class){
						return parseArray(jp,Object.class,null);
					}
					else if(typeClass.isArray()){
						return parseArray(jp,typeClass.getComponentType(),type).toArray();
					}
					else if(List.class.isAssignableFrom(typeClass)){
						Class<?> componentCls = null;
						Type componentType = null;
						if(root){
							if(type == null){
								componentCls = Object.class;
							}
							else{
								componentCls = (Class<?>)type;
							}
						}
						else{
							if(type instanceof ParameterizedType){
								ParameterizedType pt = (ParameterizedType)type;
								Type at = pt.getActualTypeArguments()[0];
								if(at instanceof ParameterizedType){
									ParameterizedType apt = (ParameterizedType)at;
									componentCls = (Class<?>) apt.getRawType();
									componentType = at;
								}
								else{
									componentCls = (Class<?>)at;
								}
							}
							else{
								componentCls = Object.class;
							}
						}
						if(componentCls == null){
							throw new JSONRequestParseException(JSONRequestParseException.TYPE_NOT_SUPPORT,"List must define parameteredType.");
						}
						return parseArray(jp,componentCls,componentType);
					}
					else{
						throw new JSONRequestParseException(JSONRequestParseException.TYPE_MISMATCH,"type[" + typeClass.getName() + "] is not array.");
					}
				
				case VALUE_STRING:
					if(typeClass == String.class || typeClass == Object.class){
						return jp.getText();
					}
					if(ReflectUtil.isSimpleType(typeClass)){
						return ConversionUtils.convert(jp.getText(), typeClass);
					}
					throw new JSONRequestParseException(JSONRequestParseException.TYPE_MISMATCH,"String value can't convert to type[" + typeClass.getName() + "]");
					
				case END_ARRAY:
				case END_OBJECT:
				case FIELD_NAME:
					throw new JSONRequestParseException(JSONRequestParseException.PARSE_ERROR);
					
				case START_OBJECT:
					if(Map.class.isAssignableFrom(typeClass) || typeClass == Object.class){
						return parseMap(typeClass,type,jp,root);
					}
					return parseObject(typeClass,jp);
	
				case VALUE_EMBEDDED_OBJECT:
					return null;
				
				case VALUE_TRUE:
				case VALUE_FALSE:
					if(typeClass == boolean.class ||  typeClass == Object.class){
						return jp.getBooleanValue();
					}
					if(ReflectUtil.isSimpleType(typeClass)){
						return ConversionUtils.convert(jp.getBooleanValue(), typeClass);
					}
					throw new JSONRequestParseException(JSONRequestParseException.TYPE_MISMATCH,"boolean value can't convert to type[" +typeClass.getName()  + "].");
					
				case VALUE_NUMBER_FLOAT:
					if(typeClass == float.class || typeClass == Object.class){
						return jp.getFloatValue();
					}
					if(ReflectUtil.isSimpleType(typeClass)){
						return ConversionUtils.convert(jp.getFloatValue(), typeClass);
					}
					throw new JSONRequestParseException(JSONRequestParseException.TYPE_MISMATCH,"float value can't convert to type[" + typeClass.getName() + "].");	
					
				case VALUE_NUMBER_INT:
					if(typeClass == int.class || typeClass == Object.class){
						return jp.getIntValue();
					}
					if(ReflectUtil.isSimpleType(typeClass)){
						return ConversionUtils.convert(jp.getIntValue(), typeClass);
					}
					throw new JSONRequestParseException(JSONRequestParseException.TYPE_MISMATCH,"int value can't convert to type[" + typeClass.getName() + "].");
					
				default:
					throw new JSONRequestParseException(JSONRequestParseException.PARSE_ERROR);
			}
			
		}
		catch(JsonParseException e){
			throw new JSONRequestParseException("json parse error.",e);
		}
		catch(IOException e){
			throw new JSONRequestParseException("json parse io error.",e);
		}
		
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Map parseMap(Class<?> typeClass,Type type, JsonParser jp, boolean root){
		Map map = new LinkedHashMap();
		
		Class<?> valueTypeClass = null;
		Type valueType = null;
		
		Class<?> keyTypeClass = String.class;;
		
		if(root){
			valueTypeClass = (Class<?>)type;
		}
		else{
			if(type instanceof ParameterizedType){
				ParameterizedType pt = (ParameterizedType)type;
				keyTypeClass = (Class<?>)pt.getActualTypeArguments()[0];
				valueType = pt.getActualTypeArguments()[1];
				if(valueType instanceof ParameterizedType){
					valueTypeClass = (Class<?>)((ParameterizedType)valueType).getRawType();
				}
				else{
					valueTypeClass = (Class<?>)valueType;
				}
			}
			else{
				valueTypeClass = Object.class;
			}
		}
		
		if(!ReflectUtil.isSimpleType(keyTypeClass)){
			throw new JSONRequestParseException(JSONRequestParseException.TYPE_NOT_SUPPORT,"Map type key must be simpleType.");
		}
		
		try{
			while(jp.nextToken() != JsonToken.END_OBJECT){
				if(jp.getCurrentToken() != JsonToken.FIELD_NAME){
					throw new JSONRequestParseException(JSONRequestParseException.PARSE_ERROR,"parse object for map field name required.");
				}
				Object key = ConversionUtils.convert(jp.getCurrentName(), keyTypeClass);
				
				jp.nextToken();
				Object value = parseValue(valueTypeClass, valueType, jp);
				map.put(key, value);
			}
			return map;
		}
		catch(JsonParseException e){
			throw new JSONRequestParseException("json parse error.",e);
		}
		catch(IOException e){
			throw new JSONRequestParseException("json parse io error.",e);
		} 
	} 

	private static Object parseObject(Class<?> typeClass, JsonParser jp){
		try{
			BeanInfo beanInfo = beans.get(typeClass);
			Object bean = beanInfo.createObject();
			
			while(jp.nextToken() != JsonToken.END_OBJECT){
				if(jp.getCurrentToken() != JsonToken.FIELD_NAME){
					throw new JSONRequestParseException(JSONRequestParseException.PARSE_ERROR,"parse object for bean [" + typeClass.getName() + "] field name required.");
				}
				String nm = jp.getCurrentName();
				if(beanInfo.hasField(nm)){
					FieldInfo fieldInfo = beanInfo.getField(nm);
					
					jp.nextToken();
					Object val = parseValue(fieldInfo.getTypeClass(), fieldInfo.getType(), jp);
					try {
						fieldInfo.setValue(bean, val);
					} 
					catch (Exception e) {
						throw new JSONRequestParseException("bean[" + typeClass.getName() + "] property[" + nm + "] set value falied.");
					}
				}
				else{
					JsonToken token = jp.nextToken();
					if(token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY){
						jp.skipChildren();
					}
				}
			}
			return bean;
		}
		catch(JsonParseException e){
			throw new JSONRequestParseException("json parse error.",e);
		}
		catch(IOException e){
			throw new JSONRequestParseException("json parse io error.",e);
		} 
		catch (ExecutionException e) {
			throw new JSONRequestParseException("json parse get beanInfo[" + typeClass.getName() + "] falied.",e);
		} 
		
	}

	private static List<?> parseArray(JsonParser jp, Class<?> componentType, Type type){
		try{
			List<Object> rs = new ArrayList<>();
			while(jp.nextToken() != JsonToken.END_ARRAY){
				rs.add(parseValue(componentType,type,jp));
			}
			return rs;
		}
		catch(JsonParseException e){
			throw new JSONRequestParseException("json parse error.",e);
		}
		catch(IOException e){
			throw new JSONRequestParseException("json parse io error.",e);
		} 
	}
	
	
}
