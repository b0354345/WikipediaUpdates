package uk.ac.ncl.cs.csc8498.httpclient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class HttpClient {

	public static void main(String[] args) {	    
		try {
			//doPostHttpRequest();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void doPostHttpRequest(){
				
			new java.util.Timer().schedule(new java.util.TimerTask() {
				@Override
				public void run() {
					
					DefaultHttpClient httpClient = new DefaultHttpClient();
					long milli_1 = System.currentTimeMillis();
					long milli_2 = milli_1 - 10000L;
	
					String startTime = DateFormatter.fomatDate(milli_1);
					String endTime = DateFormatter.fomatDate(milli_2);
	
					try {
						// Define a postRequest request
						String url = "http://en.wikipedia.org/w/api.php";
	
						HttpPost postRequest = new HttpPost(url);
						// Set the API media type in http content-type header
						postRequest.addHeader("accept", "text/xml");
						StringEntity input = new StringEntity("");
						input.setContentType("text/xml");
						postRequest.setEntity(input);
	
						List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>(
								1);
						nameValuePairs
								.add(new BasicNameValuePair("action", "query"));
						nameValuePairs.add(new BasicNameValuePair("list",
								"recentchanges"));
						nameValuePairs.add(new BasicNameValuePair("rcprop",
								"title|user|timestamp"));
						nameValuePairs.add(new BasicNameValuePair("format", "xml"));
						nameValuePairs
								.add(new BasicNameValuePair("rclimit", "max"));
						nameValuePairs.add(new BasicNameValuePair("rcstart",
								startTime));
						nameValuePairs
								.add(new BasicNameValuePair("rcend", endTime));
						postRequest.setEntity(new UrlEncodedFormEntity(
								nameValuePairs));
	
						// Send the request; It will immediately return the response
						// in HttpResponse object if any
						HttpResponse response = httpClient.execute(postRequest);
	
						// verify the valid error code first
						int statusCode = response.getStatusLine().getStatusCode();
						if (statusCode != 200) {
							throw new RuntimeException(
									"Failed with HTTP error code : " + statusCode);
						}
						BufferedReader rd = new BufferedReader(
								new InputStreamReader(response.getEntity()
										.getContent()));
	
						File file = new File("result.txt");
						OutputStreamWriter wr = new OutputStreamWriter(
								new FileOutputStream(file), "UTF-8");
						String line = "";
						String result = "";
						while ((line = rd.readLine()) != null) {
							result += line;
						}
						result = XmlFormatter.format(result);
						wr.write(result);
						wr.close();
						rd.close();
						JDOMXMLParser.parseXML("result.txt");
						System.out.println(JDOMXMLParser.getCount());
						Thread.sleep(2000);
						
							if (JDOMXMLParser.getCount() >50) {
								//System.out.println(JDOMXMLParser.getCount());
								System.out.println("Application Terminates");
								System.exit(0);
							}
						
	
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IllegalStateException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} finally {
						// Important: Close the connect
						httpClient.getConnectionManager().shutdown();
					}
				}
			}, 0, 12000);
	}
	
	public static void doGetHttpRequest() throws Exception
	{
	    DefaultHttpClient httpClient = new DefaultHttpClient();
	    try
	    {
	        //Define a HttpGet request; You can choose between HttpPost, HttpDelete or HttpPut also.
	        //Choice depends on type of method you will be invoking.
	    	String url = "http://en.wikipedia.org/w/api.php?action=query&list=recentchanges&" +
                    "rcprop=title%7Cuser%7Ctimestamp&rclimit=5&rcstart=20140224130000&rcend=20140224120000";
	        HttpGet getRequest = new HttpGet(url);
	         
	        //Set the API media type in http accept header
	        getRequest.addHeader("accept", "application/xml");
	          
	        //Send the request; It will immediately return the response in HttpResponse object
	        HttpResponse response = httpClient.execute(getRequest);
	         
	        //verify the valid error code first
	        int statusCode = response.getStatusLine().getStatusCode();
	        if (statusCode != 200)
	        {
	            throw new RuntimeException("Failed with HTTP error code : " + statusCode);
	        }
	         
	        //Now pull back the response object
	        HttpEntity httpEntity = response.getEntity();
	        String apiOutput = EntityUtils.toString(httpEntity);
	         
	        //Lets see what we got from API
	        System.out.println(apiOutput); 
	         
	    }
	    finally
	    {
	        //Important: Close the connect
	        httpClient.getConnectionManager().shutdown();
	    }
	}
	
	
	

}
