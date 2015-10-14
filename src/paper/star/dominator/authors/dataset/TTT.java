package paper.star.dominator.authors.dataset;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TTT {
	   public static void main(String[] args) {
		   Map<String, Object> logDataConfig = new HashMap<String, Object>();
		   logDataConfig.put("REDIS_HOST", "172.16.101.100");
	        logDataConfig.put("REDIS_PORT", 6379);
	        logDataConfig.put("REDIS_PASSWORD",null);
	        logDataConfig.put("REDIS_TIMEOUT", 2000);
	        logDataConfig.put("REDIS_DB", 1);
	        logDataConfig.put("REDIS_NAME", "Tracking");
	        
	        RedisLogConfigs = new HashMap();
	        RedisLogConfigs.put("REDIS_HOST", null);
	        RedisLogConfigs.put("REDIS_PORT", null);
	        RedisLogConfigs.put("REDIS_PASSWORD", null);
	        RedisLogConfigs.put("REDIS_TIMEOUT", null);
	        RedisLogConfigs.put("REDIS_DB", null);
	        RedisLogConfigs.put("REDIS_NAME", null);
	        setRedisLogConfig(logDataConfig);
	        setRedisLogConfig(logDataConfig);
//		double x=2.0d,b=0;
//		double c=x/b;
//		double a=12.0d+c;
//		if (Double.isNaN(a)) {
//			a=0.0d;
//		}
//            Integer m=121133;
//            int a=m.hashCode();
//		System.out.println(a);
//               double a=2.0d;
//               double b=3.0d;
//               
//               b=(a)/(a+b);
//               System.out.println(""+b);
	}
	   private static Map RedisLogConfigs;
	   public static void setRedisLogConfig(Map data) {
	        Map configData = new HashMap(data);
	        Iterator it = configData.entrySet().iterator();
	        while (it.hasNext()) {
	            Map.Entry pair = (Map.Entry) it.next();
	            if (RedisLogConfigs.containsKey(pair.getKey())) {
	                RedisLogConfigs.put(pair.getKey(), pair.getValue());
	            }
	            it.remove();
	        }
	        System.out.println(RedisLogConfigs.toString());
	    }
}
