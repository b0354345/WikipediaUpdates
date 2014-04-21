package uk.ac.ncl.cs.csc8498.httpclient;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class PagefEditsPerHour {
	private static Cluster cluster;
    private static Session session;
    private static  DateFormat dateFormat;
    
    public PagefEditsPerHour()
    { 
    	 dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH]");
    	 cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build(); 
		 final int numberOfConnections = 1;
		 PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();
		 poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, numberOfConnections);
		 poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, numberOfConnections);
		 poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, numberOfConnections);
		 poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, numberOfConnections);
		 final Session bootstrapSession = cluster.connect();
		 bootstrapSession.execute("CREATE KEYSPACE IF NOT EXISTS wikiproject WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
		 bootstrapSession.shutdown();
		
		 session = cluster.connect("wikiproject");
		
		 session.execute("CREATE TABLE IF NOT EXISTS no_of_edits_hour (hour bigint, title text, hits counter, PRIMARY KEY (title, hour));");	
		 //session.execute("CREATE INDEX ON no_of_edits_hour (hits);");
    }
    
	public void writeToDB() throws InterruptedException, ParseException {
		String psString = "SELECT title, edit_time FROM user_edit;";

		final int maxOutstandingFutures = 4;
		final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(
				maxOutstandingFutures);
		// prepared statement for inserting records into the table
		final PreparedStatement updatePS = session
				.prepare("UPDATE no_of_edits_hour SET hits = hits + ? WHERE title = ? AND hour = ?;");

		String title = "";
		String hour = "";
		// iterate through the result set and print the results on the console
		final ResultSetFuture queryFuture = session.executeAsync(psString);
		ResultSet resultSet = queryFuture.getUninterruptibly();
		int count = 0;
		for (Row row : resultSet) {
			title = row.getString(0);
			if (title.startsWith("User") || title.startsWith("Wikipedia") || title.startsWith("File") || title.startsWith("Talk"))
				continue;
			hour = dateFormat.format(row.getDate(1));
			Date timeStamp = (Date) dateFormat.parse(hour);
			BoundStatement boundState = new BoundStatement(updatePS).bind(1L,
					title, timeStamp.getTime());

			System.out.println(count++);

			// when the batch is full, execute asynchronously
			outstandingFutures.put(session.executeAsync(boundState));
			if (outstandingFutures.remainingCapacity() == 0) {
				ResultSetFuture resultSetFuture = outstandingFutures.take();
				resultSetFuture.getUninterruptibly();
			}
		}
		while (!outstandingFutures.isEmpty()) {
			ResultSetFuture resultSetFuture = outstandingFutures.take();
			resultSetFuture.getUninterruptibly();
		}
		cluster.shutdown();
	}
	
	
	/**
	 *  This method sends a query to a DB to return total number of accesses for each url
	 *  for a given a set of urls, start hour and end hour.
	 *  
	 *  @throws InterruptedException, ParseException
     * @throws IOException 
	 */
    public void totalAccessRead(String startHour, String endHour) throws ParseException 
   	{	
	    	// sample titles for the query
	    	String title1 = "Xunlei";
	    	String title2 = "Wikipedia:WikiProject Antarctica Highways";
	    	String title3 = "User talk:172.56.2.18";
	    	
	    	// store the titles and hits for each titled in the map
	    	Map<String, Integer> map = new HashMap<String, Integer>();
	    	map.put(title1, 0);
	    	map.put(title2, 0);
	    	map.put(title3, 0);
	    	
	    	// parse string into date object
	    	Date start = dateFormat.parse(startHour);
	    	Date end = dateFormat.parse(endHour);
	    	String psString = "SELECT title, hour, hits FROM no_of_edits_hour WHERE title in (?, ?, ?)" +
	                   " AND hour > ? AND hour <= ?;";
	    	// prepared statement for querying the DB
	   		final PreparedStatement selectPS = session.prepare(psString);	
	   		BoundStatement bs = new BoundStatement(selectPS).bind(title1, title2, title3, end.getTime(), start.getTime());
	   		System.out.println(bs);
	   		
	    	// iterate through the result set and print the results on the console
	   		final ResultSetFuture queryFuture = session.executeAsync(bs);	
	   		ResultSet resultSet = queryFuture.getUninterruptibly();	   		
	   		for (Row row : resultSet)
	   		{
	   			System.out.println(row.getString(0) + " " + dateFormat.format(row.getLong(1)) + " " + row.getLong(2));	
	   			// update the map
	   			int count = map.get(row.getString(0));
	   			count += row.getLong(2);
	   			map.put(row.getString(0), count);
	   		}
	   		
	   		for (Map.Entry<String, Integer> entry : map.entrySet()) {
	   		    System.out.println(entry.getKey() + ", " + entry.getValue());
	   		}
	   		cluster.shutdown();
   	}	
    
    public static void main(String[] args)
    {
    	PagefEditsPerHour edu = new PagefEditsPerHour();
		try {
			edu.writeToDB();
		} catch (InterruptedException | ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//edu.totalAccessRead();
    }
}
