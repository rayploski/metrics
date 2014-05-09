package org.jboss.bigcommotion.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.apache.commons.lang.StringUtils;
import org.jboss.bigcommotion.model.WebMetric;
import org.jboss.bigcommotion.util.Resources;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;

@Stateless
@LocalBean
public class AnalyticsPageViewParser {
	

	@Inject
	private transient Logger logger;	

	private static final String REGEX_COMMAS_AND_QUOTES = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
	private static final int END_OF_URI_METRICS_LINENUM = 2506;    
	SimpleDateFormat sdf = new SimpleDateFormat("MMM-dd-yyyy");


	
	@PersistenceContext(unitName = Resources.PERSISTENCE_CONTEXT_NAME)
	private EntityManager em;

	private static KieServices ks = KieServices.Factory.get();
    private KieContainer kContainer = ks.getKieClasspathContainer();
    
    
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
    private void addOrUpdateMetric(Map<String, WebMetric> metrics, WebMetric metric)
    {
    	assert metric != null : "metric must be specified.";
    	assert metrics != null : "metrics must be specified";

    	StatelessKieSession sSession = kContainer.newStatelessKieSession("webmetrics-stateless");
    	sSession.execute(Arrays.asList(new Object[]{metric}));
    	String page = metric.getPage();

    	if(page.length() > 511)
    		page = page.substring(1,510);
    	
    	if (page.equalsIgnoreCase("/jbossorg-downloads/JBoss-6.0.0.Final") 
    			|| page.equalsIgnoreCase("/jbossorg-downloads/JBoss-5.1.0.GA"))
    		page = "/jbossas/downloads";
    	if ( StringUtils.startsWith(page, "/tools/download/"))
    		page = "/tools/download/";
    	
    	// Only URL that should have a trailing '/' is the root.
    	if (StringUtils.endsWith(page, "/") && page.length() > 1){
    		page = (StringUtils.substringBeforeLast(page, "/"));
    	}
    	
    	if (metric.getSite().equals("jboss.org")){
        	if (page.equals("/docs/EAPdocumentation"))
        		metric.setProject("EAP");
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
    
    @TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
    private void saveMetrics(Map<String,WebMetric> metrics){
    	assert metrics != null : "metrics must be specified";
    	for (String key : metrics.keySet()){
        	WebMetric metric = metrics.get(key);
        	em.persist(metric);
        }
    	em.flush();
    }
    
	
    /**
     * Parses Google Analytics Standard PageView report, consolidates duplicate entries and persists them
     * to the database
     * @param file file to be parsed
     * @param startDate start date of the report
     * @return
     * @throws Exception
     */
    public Collection<WebMetric> parseFile(String siteName, File file, Date startDate) throws Exception{
    	assert siteName != null : "siteName must be specified";
    	assert file != null : "file must be specified";
    	assert startDate !=null : "startDate must be specified";
        
    	Map<String, WebMetric> metrics = new HashMap<String, WebMetric>();  //stores paths for consolidating things like /downloads and /downloads/index.html prior to pertisting to the DB.
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedFileReader = new BufferedReader(fileReader);
    	Scanner scanner = new Scanner(bufferedFileReader);
		long lineNum = 0;
        // Skip the metadata for now.  TODO:  Add metadata to the model.
        while (lineNum < 7){
            scanner.nextLine();
        	lineNum++;
        }

        scanner.useDelimiter(REGEX_COMMAS_AND_QUOTES);
        
        // Scan each line.  
        // TODO: This logic is incorrect.  Many of the secondary sites do not have 2500 entries.  Only jboss.org does.
        logger.info("Parsing metrics from " + sdf.format(startDate) + " for " + siteName + "...");
        filescan:
        	while(scanner.hasNextLine() && lineNum < END_OF_URI_METRICS_LINENUM )
        	{
        		while(scanner.hasNext()){
        			WebMetric metric = new WebMetric();
        			metric.setDate(startDate);
        			String url = scanner.next();
        			metric.setPage(url);
        			metric.setSite(siteName);
        			metric.setFileName(file.getAbsolutePath());
        			metric.setProject(file.getParentFile().getName());

        			try {
        				metric.setPageViews(Resources.stripQuotes(scanner.next()));
        				metric.setUniquePageViews(Resources.stripQuotes(scanner.next()));
        				scanner.next(); // omit average time on page for now.
        				metric.setEntrances(Resources.stripQuotes(scanner.next()));
        				metric.setBounceRate(Resources.parsePercentage(scanner.next())); 
        				metric.setPercentExit(Resources.parsePercentage(scanner.next()));
        				lineNum++;
        				scanner.nextLine(); //omit page value for now
        				logger.log(Level.FINEST,"WebMetric = " + metric.toString());
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
 
        //TODO:  Add summarized page-views that start on line 2511 of a JBoss.org report.
        //TODO:  Address and recognize pattern for the end of the individual files.  We *do* want to record the rest of the file but this will do for now.
        scanner.close();
        
        logger.info("Saving metrics from " + sdf.format(startDate) + " recording " + metrics.size() + " metrics");
        saveMetrics(metrics);
        return metrics.values();
    }

    
}
