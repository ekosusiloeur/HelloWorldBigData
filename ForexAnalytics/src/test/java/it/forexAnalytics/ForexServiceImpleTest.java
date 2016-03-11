package it.forexAnalytics;

import static org.junit.Assert.assertTrue;
import it.forex.model.ForexData;
import it.forex.services.ForexService;
import it.forex.servicesImpl.ForexServiceImpl;

import org.junit.BeforeClass;
import org.junit.Test;

public class ForexServiceImpleTest {

	private static ForexService fx;
	@BeforeClass
	public static void init(){
		fx=new ForexServiceImpl();
		
	}
	@Test
	public void testStoreGetDeleteData(){
		ForexService fx=new ForexServiceImpl();
		ForexData data=new ForexData();
		data.setInstrument("EUR_IDR");
		data.setTimeStamp(1457718575151l);
		data.setBuyPrice(15236);
		data.setSellPrice(16223);
		fx.store(data);
		assertTrue(fx.getData(data.getInstrument(), data.getTimeStamp())!=null);
	}
	

	
	@Test
	public void testScanData(){
		fx.scan("EUR_USD");
	}
	
	
}
