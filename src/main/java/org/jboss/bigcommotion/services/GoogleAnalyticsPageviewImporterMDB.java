package org.jboss.bigcommotion.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.inject.Inject;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.MessageListener;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.apache.commons.lang.StringUtils;
import org.jboss.bigcommotion.model.WebMetric;
import org.jboss.bigcommotion.util.Resources;

@MessageDriven(name="PageviewParserMDB", activationConfig = {
		@ActivationConfigProperty(propertyName="destinationType", propertyValue="queue"),
		@ActivationConfigProperty(propertyName="destination", propertyValue=Resources.PAGEVIEW_QUEUE),
		@ActivationConfigProperty(propertyName="acknowledgeMode", propertyValue="auto-acknowledge")
		})
public class GoogleAnalyticsPageviewImporterMDB implements MessageListener{
	
   private static final String REGEX_COMMAS_AND_QUOTES = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
   private static final int END_OF_URI_METRICS_LINENUM = 2506;


	@PersistenceContext(unitName = Resources.PERSISTENCE_CONTEXT_NAME)
	private EntityManager em;
	    
	@Inject
	private transient Logger logger;

	@Override
	public void onMessage(Message rcvMessage) {
	
		ObjectMessage msg = null;
		
		try {
			if (rcvMessage instanceof ObjectMessage)
			{
				msg = (ObjectMessage)rcvMessage;
				if (msg.getObject() instanceof WebMetric){
					WebMetric m = (WebMetric)msg.getObject();
					logger.info("Received message from queue " +  m.toString());
					parseFile(m.getSite(), new File(m.getFileName()), m.getDate());
				}
			}
			
		} catch (JMSException jmsE){
			logger.log(Level.SEVERE, "Issue processing message. ", jmsE);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Issue processing file: " , e);
		}
		
	}
	
    /**
     * Parses Google Analytics Standard PageView report, consolidates duplicate entries and persists them
     * to the database
     * @param file file to be parsed
     * @param startDate start date of the report
     * @return
     * @throws Exception
     */
    private Collection<WebMetric> parseFile(String siteName, File file, Date startDate) throws Exception{
    	assert siteName != null : "siteName must be specified";
    	assert file != null : "file must be specified";
    	assert startDate !=null : "startDate must be specified";
        
    	Map<String, WebMetric> metrics = new HashMap<String, WebMetric>();

        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedFileReader = new BufferedReader(fileReader);
    	Scanner scanner = new Scanner(bufferedFileReader);
		long lineNum = 0;
		
        // Skip the metadata for now.
        while (lineNum < 7){
            scanner.nextLine();
        	lineNum++;
        }

        scanner.useDelimiter(REGEX_COMMAS_AND_QUOTES);
        
        // Scan each line.  
        // TODO: This logic is incorrect.  Many of the secondary sites
        // do not have 2500 entries.  Only jboss.org does.
        
        logger.info("Parsing metrics from " + startDate + "...");
        filescan:
        	while(scanner.hasNextLine() && lineNum < END_OF_URI_METRICS_LINENUM )
        	{
        		while(scanner.hasNext()){
        			WebMetric metric = new WebMetric();
        			metric.setDate(startDate);
        			String url = scanner.next();
        			metric.setPage(url);
        			metric.setSite(siteName);

        			try {
        				metric.setPageViews(Resources.stripQuotes(scanner.next()));
        				metric.setUniquePageViews(Resources.stripQuotes(scanner.next()));
        				scanner.next(); // omit average time on page for now.
        				metric.setEntrances(Resources.stripQuotes(scanner.next()));
        				metric.setBounceRate(Resources.parsePercentage(scanner.next())); 
        				metric.setPercentExit(Resources.parsePercentage(scanner.next()));
        				lineNum++;
        				scanner.nextLine(); //omit page value for now
        				logger.fine("WebMetric = " + metric.toString());
        				addOrUpdateMetric(metrics, metric);
        			} 
        			catch (java.util.NoSuchElementException nse){
        				logger.warning("Scanner shit the bed at line: " + lineNum + "Stopping scan of file.");
        				break filescan;
        			}
        			catch (NumberFormatException nfe)
        			{
        				logger.warning( "Issue with " + metric.getPage() + " in " + metric.getDate() 
        						+ " at line " +  lineNum + ".  Stopping Scan");
        				break filescan;
        			}            
        		}  // End of Line Scan
        	} // End of File Scan
 
        //TODO:  Add summarized page-views that start on line 2511
        scanner.close();
        
        logger.info("Saving metrics from " + startDate + " recording " + metrics.size() + " metrics");
        saveMetrics(metrics);
    	    
        return metrics.values();
    }
    
    private void saveMetrics(Map<String,WebMetric> metrics){

    	assert metrics != null : "metrics must be specified";
    	
    	for (String key : metrics.keySet()){
        	WebMetric metric = metrics.get(key);
        	em.persist(metric);
        }
    	em.flush();
    }
    

    /**
     * There are many duplicates from jboss.org.  This method checks existing entries,
     * and confirms there is only one.  Should a duplicate entry exist, the method
     * sums the visits, entrances, unique pageviews and exits.  
     * TODO:  Still need to do the moving averages correctly.
     * TODO:  Parse the average time on page into seconds, then add to the metric
     * TODO:  Associate project directly by the path name when possible.
     * TODO:  Possibly make this JDG/ISPN based?
     * @param metrics
     * @param metric
     */
    private static void addOrUpdateMetric(Map<String, WebMetric> metrics, WebMetric metric)
    {
    	assert metric != null : "metric must be specified.";
    	assert metrics != null : "metrics must be specified";
    	
    	String page = metric.getPage();
    	
    	//TODO: These rules would be way better in a drools spreadsheet.
    	page = page.length() > 512 ? page.substring(1, 511):page;
    	page = page.contains("?") ? StringUtils.substringBeforeLast(page, "?"):page;
    	page = StringUtils.endsWith(page, "download.html")?StringUtils.substringBeforeLast(page, ".html"):page;
    	page = StringUtils.endsWith(page, "index.html")?StringUtils.substringBeforeLast(page, "index.html"):page;
    	page = StringUtils.endsWith(page,"tools.html")?StringUtils.substringBeforeLast(page,".html"):page;
    	page = StringUtils.endsWith(page,".html")?StringUtils.substringBeforeLast(page,".html"):page;

    	if (page.equalsIgnoreCase("/jbossorg-downloads/JBoss-6.0.0.Final") 
    			|| page.equalsIgnoreCase("/jbossorg-downloads/JBoss-5.1.0.GA"))
    		page = "/jbossas/downloads";
    	if ( StringUtils.startsWith(page, "/tools/download/"))
    		page = "/tools/download/";
    	
    	// Only URL that should have a trailing '/' is the root.
    	if (StringUtils.endsWith(page, "/") && page.length() > 1){
    		page = (StringUtils.substringBeforeLast(page, "/"));
    	}
    	
    	if (metrics.containsKey(page)){

    		// Add the metrics together
    		WebMetric existingMetric = metrics.get(page);
    		existingMetric.addMetrics(metric);
    	} else {
    		metric.setPage(page);
    		metrics.put(page, metric);
    	}    	
    }    
	
	


	
}
