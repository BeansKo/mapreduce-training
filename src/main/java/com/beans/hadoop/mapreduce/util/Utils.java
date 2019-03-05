package com.beans.hadoop.mapreduce.util;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;

public class Utils {
	public static boolean isEmpty(Object obj) {
		if (obj == null) {
			return true;
		} else if (obj instanceof String) {
			return "".equals(String.valueOf(obj).trim());
		} else if (obj instanceof Map<?, ?>) {
			return ((Map<?, ?>) obj).isEmpty();
		} else if (obj instanceof Collection<?>) {
			return ((Collection<?>) obj).isEmpty();
		} else if(obj instanceof Array){
			return Array.getLength(obj)==0;
		}
		return false;
	}
	
	public static boolean isNotEmpty(Object obj){
		return !isEmpty(obj);
	}
}
