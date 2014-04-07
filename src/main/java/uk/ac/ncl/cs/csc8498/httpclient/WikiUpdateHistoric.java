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

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;

public class WikiUpdateHistoric {
	private static long START_MILLI = System.currentTimeMillis() + 300000L; //System.currentTimeMillis() + 300000L;
	private static long END_MILL = START_MILLI - 300000L; // 5 minute window

	public static void main(String[] args) {
		try {
			doPostHttpRequestHistoric();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void doPostHttpRequestHistoric() {
		DefaultHttpClient httpClient = new DefaultHttpClient();
		//long count = JDOMXMLParser.getCount();
		while (JDOMXMLParser.getCount() < 1000000000) {
			
			START_MILLI -= 300000L;
			END_MILL = START_MILLI - 300000L;

			
			String startTime = DateFormatter.fomatDate(START_MILLI);
			String endTime = DateFormatter.fomatDate(END_MILL);

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
				nameValuePairs.add(new BasicNameValuePair("action", "query"));
				nameValuePairs.add(new BasicNameValuePair("list",
						"recentchanges"));
				nameValuePairs.add(new BasicNameValuePair("rcprop",
						"title|user|timestamp|comment|userid|ids|flags|loginfo"));
				nameValuePairs.add(new BasicNameValuePair("format", "xml"));
				nameValuePairs.add(new BasicNameValuePair("rclimit", "max"));
				nameValuePairs
						.add(new BasicNameValuePair("rcstart", startTime));
				nameValuePairs.add(new BasicNameValuePair("rcend", endTime));
				postRequest.setEntity(new UrlEncodedFormEntity(nameValuePairs));

				// Send the request; It will immediately return the response
				// in HttpResponse object if any
				HttpResponse response = httpClient.execute(postRequest);

				// verify the valid error code first
				int statusCode = response.getStatusLine().getStatusCode();
				if (statusCode != 200) {
					throw new RuntimeException("Failed with HTTP error code : "
							+ statusCode);
				}
				BufferedReader rd = new BufferedReader(new InputStreamReader(
						response.getEntity().getContent()));

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
				System.out.println(START_MILLI);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				System.out.println(START_MILLI);
			} catch (IllegalStateException e) {
				// TODO Auto-generated catch block
				System.out.println(START_MILLI);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println(START_MILLI);
			} 
				
			
		}
		// Important: Close the connect
		httpClient.getConnectionManager().shutdown();
		JDOMXMLParser.cluster.cleanup();
	}

}
