package org.jboss.bigcommotion.services;

import org.apache.commons.lang.StringUtils;
import org.jboss.bigcommotion.model.WebMetric;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

/**
 * Imports a CSV formatted Google Analytics pageview report based on standard defaults.
 */
@Stateless
public class GoogleAnalyticsImportService {

	
   @PersistenceContext(unitName = "forge-default")
   private EntityManager em;


	
    private static final int END_OF_URI_METRICS_LINENUM = 2506;
    
    @Inject
    private transient Logger logger;

    
	public void go(final String siteName, final String metricDir){

        File folder = new File(metricDir);

        if (!folder.isDirectory()){
            System.out.println("Cannot continue.  Must be a directory path");
            return;
        }

        File[] files = folder.listFiles();
        for (int i = 0; i < files.length; i++){
            importPageViewMetrics(siteName,files[i]);
        }
    }


    public void importPageViewMetrics(final String siteName, final String filePath){
        File file = new File(filePath);
        importPageViewMetrics(siteName, filePath);
	}
	
	public void importPageViewMetrics(final String siteName, File file){

		//omit Mac specific .DS_Store file
		if (StringUtils.endsWith(file.getName(), ".DS_Store")){
			return;
		}
		Date startDate = getStartDate(file.getName());		
		try {
			parseFile(file, startDate);
		} catch (Exception e){
			logger.log(Level.SEVERE, "Error parsing file: ", e);
		}
		
		//Open the file, and read each row.
		
		return;
	}

    /**
     *  Scrapes filename for the month of the metrics.  By default, the Google Analytics naming convention is
     *  similar to "Analytics [site-name] Pages YYYYMMDD-YYMMDD" where the first date is the start of the report and
     *  the second date is the end of the report.
     **/
    private Date getStartDate(final String fileName) {

        String dateRange = StringUtils.substringAfterLast(fileName, " ");
        String startDateStr = StringUtils.substringBefore(dateRange, "-");
        SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMdd");
        Date startDate = null;

        try {
        	logger.log(Level.FINE, "********** dateRange = " + dateRange);
            logger.log(Level.FINE, "********** startDateStr = " + startDateStr);
            startDate = sdf.parse(startDateStr);
        } catch (ParseException pe){
        	
        	pe.printStackTrace();
        }

        return startDate;

    }

    private Date getEndDate(final String fileName){
    	String dateRange = StringUtils.substringAfterLast(fileName, " ");
    	String endDateStr = StringUtils.substringBeforeLast((StringUtils.substringAfter(dateRange, "-")), ".");
    	SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMdd");
    	Date endDate = null;
    	try {
    		endDate = sdf.parse(endDateStr);
    	} catch (ParseException  pe){
    		pe.printStackTrace();
    	}
    	
    	return endDate;
    }


    private List<WebMetric> parseFile(File file, Date startDate) throws Exception{
        
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

        scanner.useDelimiter(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        
        while(scanner.hasNextLine() && lineNum < END_OF_URI_METRICS_LINENUM ){
            while(scanner.hasNext()){
            	WebMetric metric = new WebMetric();
                metric.setDate(startDate);
                String url = scanner.next();
                metric.setPage(url);
                
            	try {
            		metric.setPageViews(stripQuotes(scanner.next()));
                    metric.setUniquePageViews(stripQuotes(scanner.next()));
                    scanner.next(); // omit average time on page for now.
                    metric.setEntrances(stripQuotes(scanner.next()));
                    metric.setBounceRate(parsePercentage(scanner.next())); 
                    metric.setPercentExit(parsePercentage(scanner.next()));
                    lineNum++;
                    scanner.nextLine(); //omit page value for now
                    logger.log(Level.FINER, metric.toString());
                    addOrUpdateMetric(metrics, metric);
            	} 
            	catch (java.util.NoSuchElementException nse){
            		logger.log(Level.SEVERE,"Scanner shit the bed after line: " + lineNum);
            		nse.printStackTrace();	
            	}
            	catch (NumberFormatException nfe)
            	{
            		logger.log(Level.WARNING, "Issue with " + metric.getPage() + " in " + metric.getDate() + ": ");
            		nfe.printStackTrace();
            	}
            	
            	
            }
            
            for (String key : metrics.keySet()){
            	WebMetric metric = metrics.get(key);
            	metric = em.merge(metric);
            }
        }
            
        //TODO:  Add summarized page-views that start on line 2511
        scanner.close();
    	    
        return null;
    }
    

    private void addOrUpdateMetric(Map<String, WebMetric> metrics, WebMetric metric)
    {
    	// Remove cruft from name
    	String page = metric.getPage();
    	
    	// Only URL that should have a trailing '/' is the root.
    	if (StringUtils.contains(page, "/") && page.length() > 0){
    		page = (StringUtils.substringBeforeLast(page, "/"));
    	}
    	
    	page = StringUtils.substringBeforeLast(page, ".html");
    	
    	if (metrics.containsKey(page)){
    		// Add the metrics together
    		WebMetric existingMetric = metrics.get(page);
    		existingMetric.addMetrics(metric);
    	} else {
    		metric.setPage(page);
    		metrics.put(page, metric);
    	}
    	
    }
    
    private static long stripQuotes(String value){
    	String temp = StringUtils.remove(value, "\"");
    	temp = StringUtils.remove(temp,",");
    	return new Long(temp).longValue();
    }
    
    private static Float parsePercentage(String value){
    	String temp = StringUtils.remove(value, "%");
    	return new Float(temp);
    }
    
    
    
}





