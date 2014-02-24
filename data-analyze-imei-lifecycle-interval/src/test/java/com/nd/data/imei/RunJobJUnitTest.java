package com.nd.data.imei;

import com.nd.data.imei.ImeiLifecycleIntervalJobStart;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author aladdin
 */
public class RunJobJUnitTest {
    
    public RunJobJUnitTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }
    
     @Test
     public void runJob() throws Exception {
         String[] args = {
             "stateDate=2013-10-01",
             "outputPath=/user/liujy/imei-lifecycle-interval-20131001-11"
         };
         ImeiLifecycleIntervalJobStart.main(args);
     }
}