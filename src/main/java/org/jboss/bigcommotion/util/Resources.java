package org.jboss.bigcommotion.util;


import java.util.logging.Logger;

import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;

import org.apache.commons.lang.StringUtils;



public class Resources {

	public static final String PERSISTENCE_CONTEXT_NAME = "metrics-big-commotion";
	public static final String PAGEVIEW_QUEUE = "queue/PageviewQueue";
	
	@Produces
	public Logger produceLogger(InjectionPoint ip){
		return Logger.getLogger(ip.getMember().getDeclaringClass().getName());
	}

	
    /**
     * 
     * @param value
     * @return
     */
    public static long stripQuotes(String value){
    	String temp = StringUtils.remove(value, "\"");
    	temp = StringUtils.remove(temp,",");
    	return new Long(temp).longValue();
    }
    
    /**
     * 
     * @param value
     * @return
     */
    public static Float parsePercentage(String value){
    	String temp = StringUtils.remove(value, "%");
    	return new Float(temp);
    }
    
}
