package org.jboss.bigcommotion;

import org.jboss.bigcommotion.model.WebMetric;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;


import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;




public class TestDrools {

	private static String file = "/opt/data/jboss.org/Analytics jboss.org Pages 20131101-20131130.csv";
	private String site = "www.jboss.org";
	private static KieContainer kContainer;
	KieSession ksession;

	@BeforeClass
	public static void setUp(){
		KieServices ks = KieServices.Factory.get();
	    kContainer = ks.getKieClasspathContainer();
	    System.out.println(kContainer.verify().getMessages().toString());		
	}
	
	@Before
	public void before(){
		 ksession = kContainer.newKieSession("webmetrics");	        
		 
	}
	
	@Test
	public void testRuntime(){
		String version = System.getProperty("java.version");
		Assert.assertTrue("tests must run upon java 1.6 or 1.7", (version.startsWith("1.6") || version.startsWith("1.7")));
	}
	
	
	@After
	public void after(){
		ksession.dispose();
	}
	
	
	@Test
	public void testProjectAssignmentDecisionTable() {
		WebMetric metric = new WebMetric(file,site,null);
		metric.setProject("jboss.org");
		metric.setPage("/as7.html");
        ksession.insert(metric);
        ksession.fireAllRules();
        Assert.assertEquals("/as7 should be assigned to jbossas project", metric.getProject(),"jbossas");
	}
	
	@Test
	public void testPathAssignmentMods() {
		WebMetric metric = new WebMetric(file,site,null);
		metric.setPage("as/download.html");
		ksession.insert(metric);
		ksession.fireAllRules();
		Assert.assertEquals(".html should be removed", metric.getPage(), "as/download");
	}
	
	@Test
	public void testPathQuerystringMods(){
		WebMetric metric = new WebMetric(file,site,null);
		metric.setPage("as/download?1234");
		ksession.insert(metric);
		ksession.fireAllRules();
		System.out.println("Test QueryString: " + metric.getPage());
		Assert.assertEquals("QueryStrings should be removed", metric.getPage(), "as/download");		
	}

}
