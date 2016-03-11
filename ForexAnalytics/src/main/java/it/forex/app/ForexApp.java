package it.forex.app;

import it.forex.model.ForexData;
import it.forex.services.ForexService;
import it.forex.servicesImpl.ForexServiceImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ForexApp {
	private static final String AUTHENTICATE_KEY = " ";
	private static final String USER_ID = " ";
	
	private static final String INSTRUMENT = "EUR_USD";
	private static final String DOMAIN = "https://stream-fxpractice.oanda.com";;
	private static final String TIME_FORMAT="yyyy-MM-dd'T'HH:mm:ss.SSSSSS";
	private static Logger logger = LoggerFactory.getLogger(ForexApp.class);

	private static ForexService service=new ForexServiceImpl();
	
	public static void main(String[] args) {
		System.out.println("ASDASD");
		//service.scan("EUR_USD_10:03:2016");
		read();
		
	}

	public static long parseStringTimeToLong(String strFormat, String strTime) {
		long _result = 0;
		if (strFormat != null && strTime != null) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(strFormat);
				Date date = sdf.parse(strTime);
				_result = date.getTime();
			} catch (ParseException e) {
				logger.error("Error Parsing the date");
			}
		}
		return _result;
	}

	public static ForexData parseLine(String responseLine) {
		ForexData _result = null;

		if (responseLine != null) {
			Object obj = JSONValue.parse(responseLine);
			if (obj != null) {
				JSONObject tick = (JSONObject) obj;

				if (tick.containsKey("tick")) {
					tick = (JSONObject) tick.get("tick");
					String instrument = tick.get("instrument").toString();
					String strTime = tick.get("time").toString();
					long time=parseStringTimeToLong(TIME_FORMAT,strTime);
					double bid = Double.parseDouble(tick.get("bid")
							.toString());
					double ask = Double.parseDouble(tick.get("ask")
							.toString());
					_result=new ForexData(instrument,bid,ask,time);
					service.store(_result);
					
					System.out.println(_result);
				}
			} else {
				logger.error("Error during parsing the response "
						+ responseLine);
			}

		}

		return _result;
	}

	public static void read() {

		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpUriRequest httpGet = new HttpGet(DOMAIN + "/v1/prices?accountId="
				+ USER_ID + "&instruments=" + INSTRUMENT);
		httpGet.setHeader(new BasicHeader("Authorization", "Bearer "
				+ AUTHENTICATE_KEY));

		System.out.println("Executing request: " + httpGet.getRequestLine());

		try {
			HttpResponse resp = httpClient.execute(httpGet);
			HttpEntity entity = resp.getEntity();

			if (resp.getStatusLine().getStatusCode() == 200 && entity != null) {
				InputStream stream = entity.getContent();
				String line;
				BufferedReader br = new BufferedReader(new InputStreamReader(
						stream));

				while ((line = br.readLine()) != null) {

					parseLine(line);
				}
			} else {
				
				String responseString = EntityUtils.toString(entity, "UTF-8");
				logger.error("ERROR RESPONSE " + responseString);
			}

		} catch (IOException e) {
			logger.error("EXCEPTION when reading service");
		}
		

	}

}
