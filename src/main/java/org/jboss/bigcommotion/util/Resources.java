package org.jboss.bigcommotion.util;


import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.logging.Logger;

import javax.enterprise.inject.Produces;
import javax.enterprise.inject.spi.InjectionPoint;

import org.apache.commons.lang.StringUtils;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderError;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.decisiontable.ExternalSpreadsheetCompiler;
import org.drools.io.ResourceFactory;
import org.drools.io.impl.ByteArrayResource;
import org.drools.runtime.StatelessKnowledgeSession;


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
	
    @Produces
    public StatelessKnowledgeSession produceKSession() {
    	ExternalSpreadsheetCompiler converter = new ExternalSpreadsheetCompiler();
    	String baseProjectDRL = null;
    	try {
            //the data we are interested in starts at row 10, column 3
    		 baseProjectDRL = converter.compile(getSpreadsheetStream(),getTemplateStream(), 10,3 );
    	} catch (IOException e){
    		throw new IllegalArgumentException("Invalid spreadsheet stream", e);
    	}
    	
        //compile the drls
    	KnowledgeBuilder kBuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
    	kBuilder.add(new ByteArrayResource(baseProjectDRL.getBytes()), ResourceType.DRL);

    	//compilation errors?
    	if (kBuilder.hasErrors()){
    		 System.out.println("Error compiling resources:");
             Iterator<KnowledgeBuilderError> errors = kBuilder.getErrors().iterator();
             while (errors.hasNext()) {
                 System.out.println("\t" + errors.next().getMessage());
             }
             throw new IllegalStateException("Error compiling resources");
    	}
    	
    	KnowledgeBase kBase = KnowledgeBaseFactory.newKnowledgeBase();
    	kBase.addKnowledgePackages(kBuilder.getKnowledgePackages());
    	
    	return kBase.newStatelessKnowledgeSession();
    	
    }
    
    
    
    private InputStream getTemplateStream() throws IOException {
    	return ResourceFactory.newClassPathResource("org/jboss/bigcommotion/decisiontable/MetricProjectRules.drt").getInputStream();
	}


	private InputStream getSpreadsheetStream() throws IOException {
        return ResourceFactory.newClassPathResource("org/jboss/bigcommotion/decisiontable/MetricProjectRules.xls").getInputStream();
    }
}
