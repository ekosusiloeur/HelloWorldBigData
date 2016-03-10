package it.forex.servicesImpl;

import it.forex.dao.ForexDao;
import it.forex.model.ForexData;
import it.forex.services.ForexService;

import java.util.List;
import java.util.LinkedList;



public class ForexServiceImpl implements ForexService{

	private ForexDao dao;
	
	public ForexServiceImpl(){
		dao=new ForexDao();
		dao.init();
	}
	
	
	public double getBuy(String instrument, long timeStamp) {
		// TODO Auto-generated method stub
		return 0;
	}

	public double getSell(String instrument, long timeStamp) {
		// TODO Auto-generated method stub
		return 0;
	}

	

	public void store(ForexData forexData) {
		dao.storeData(forexData);
	}	

	public List<ForexData> scanWithinTimeRange(String instrument,
			long startTime, long endTime) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public List<ForexData> scan(String instrument) {
		dao.scanAll(instrument);
		return null;
	}

}
