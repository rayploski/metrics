package org.jboss.bigcommotion;

import junit.framework.Assert;
import org.jboss.bigcommotion.model.WebMetric;
import org.junit.Before;
import org.junit.Test;

/**
 * Ensures the business logic in adding metrics is sound
 *
 * @author <a href="mailto:alr@jboss.org">ALR</a>
 */
public class AddMetricsWithNoValuesTestCase {

    private WebMetric metric1 = new WebMetric(null, null, null);
    private WebMetric metric2 = new WebMetric(null, null, null);

    @Before
    public void initMetrics(){
        metric1.setBounceRate(0.0f);
        metric1.setPageViews(0L);
        metric1.setPercentExit(0.0f);
        metric1.setEntrances(0L);
        metric1.setUniquePageViews(0L);

        metric2.setBounceRate(0.0f);
        metric2.setPageViews(0L);
        metric2.setPercentExit(0.0f);
        metric2.setEntrances(0L);
        metric2.setUniquePageViews(0L);

        metric1.addMetrics(metric2);
    }

    @Test
    public void percentExitShouldBe0(){
        Assert.assertEquals("Percent exit should be 0",0L,metric1.getPercentExit());
    }

    @Test
    public void bounceRateShouldBe0(){
        Assert.assertEquals("Bounce rate should be 0",0L,metric1.getBounceRate());
    }

}
