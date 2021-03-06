package uk.ac.ncl.cs.csc8498.cassandra_model;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import uk.ac.ncl.cs.csc8498.httpclient.ValueComparator;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Given a start hour and end hour, return titles for all the documents that were edited within that period
 * and for each page, return number of times it has been edited.
 * @author b0354345
 *
 */
public class UserEditsPerHour {

	private static Cluster cluster;
    private static Session session;
    private static  DateFormat dateFormat;
    
    public UserEditsPerHour()
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
		 session.execute("CREATE TABLE IF NOT EXISTS user_edits_per_hour (hour bigint, user text, hits counter, PRIMARY KEY (user, hour));");	
    }
    
    /**
     * Create a table with USER and hour columns as compound primary key, and a counter column 
     * to show  number of times a user edited pages  within that hour.
     * @throws InterruptedException
     * @throws ParseException
     */
	public void writeToDB() throws InterruptedException, ParseException {
		String psString = "SELECT user, title, edit_time FROM user_edit;";

		final int maxOutstandingFutures = 4;
		final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(
				maxOutstandingFutures);
		// prepared statement for inserting records into the table
		final PreparedStatement updatePS = session
				.prepare("UPDATE user_edits_per_hour SET hits = hits + ? WHERE user = ? AND hour = ?;");

		String user = "";
		String title = "";
		String hour = "";
		// iterate through the result set and print the results on the console
		final ResultSetFuture queryFuture = session.executeAsync(psString);
		ResultSet resultSet = queryFuture.getUninterruptibly();
		int count = 0;
		for (Row row : resultSet) {
			user = row.getString(0);
			title = row.getString(1);
			if (user.toLowerCase().contains("bot")) // eliminate bots
				continue;
			if (title.startsWith("User") || title.startsWith("Wikipedia") || title.startsWith("File") 
					|| title.startsWith("Talk")|| title.startsWith("Template"))
				continue;
			hour = dateFormat.format(row.getDate(2));
			Date timeStamp = (Date) dateFormat.parse(hour);
			BoundStatement boundState = new BoundStatement(updatePS).bind(1L,
					user, timeStamp.getTime());

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
		cleanup();
	}
	
	/**
	 * Given a start hour and an end hour, this method returns users and number of times
	 * each user has edited a page between the two hours. 
	 * @throws InterruptedException, ParseException
     * @throws IOException 
	 */
    public void testAllUserEditsPerHour(String startHour, String endHour) throws ParseException 
   	{		
	    	// store the titles and hits for each titled in the map
	    	Map<String, Integer> map = new HashMap<String, Integer>();
	    	
	    	// parse string into date object
	    	Date start = dateFormat.parse(startHour);
	    	Date end = dateFormat.parse(endHour);
	    	String psString = "SELECT user, hits FROM user_edits_per_hour WHERE hour > ? AND hour <= ? ALLOW FILTERING;";
	    	// prepared statement for querying the DB
	   		final PreparedStatement selectPS = session.prepare(psString);	
	   		BoundStatement bs = new BoundStatement(selectPS).bind( start.getTime(), end.getTime());
	   
	    	// iterate through the result set and print the results on the console
	   		final ResultSetFuture queryFuture = session.executeAsync(bs);	
	   		ResultSet resultSet = queryFuture.getUninterruptibly();	  
	   		String user = "";
	   		long hits = 0;
	   		int count = 0;
	   		for (Row row : resultSet)
	   		{
	   			// update the map
	   			user = row.getString(0);
	   			hits = row.getLong(1);
	   			
	   			if (map.get(user) == null)
	   			{
	   				map.put(user, (int)hits);
	   				continue;
	   			}
	   			count = map.get(user);
	   			count += hits;
	   			map.put(user, count);
	   		}
	   		
	   		ValueComparator vc = new ValueComparator(map);
	   		TreeMap<String, Integer> treeMap = new TreeMap<String, Integer>(vc);
	   		treeMap.putAll(map);
	   		int size = 0;
	   		int value = 0;
	   		for (Map.Entry<String, Integer> entry : treeMap.entrySet()) {
	   			value = entry.getValue();
	   			if (value == 1)
	   				continue;
	   		    System.out.println(entry.getValue());
	   		 size++;
	   		    if (size > 100)
	   		    	break;
	   		}
	   		cleanup();
   	}
    

	/**
	 * Given a set of users, a start hour and an end hour, this method returns number of times
	 * each user has edited documents between the two hours. 
	 * @throws InterruptedException, ParseException
     * @throws IOException 
	 */
    public void usersBetweenHours(String startHour, String endHour) throws ParseException 
   	{	
	    	// sample titles for the query
	    	String user1 = "216.51.224.101";
	    	String user2 = "Nguyen khoi";
	    	String user3 = "Ignacitum";
	    	
	    	// store the titles and hits for each titled in the map
	    	Map<String, Integer> map = new HashMap<String, Integer>();
	    	map.put(user1, 0);
	    	map.put(user2, 0);
	    	map.put(user3, 0);
	    	
	    	// parse string into date object
	    	Date start = dateFormat.parse(startHour);
	    	Date end = dateFormat.parse(endHour);
	    	String psString = "SELECT user, hits FROM user_edits_per_hour WHERE user in (?, ?, ?)" +
	                   " AND hour > ? AND hour <= ?;";
	    	// prepared statement for querying the DB
	   		final PreparedStatement selectPS = session.prepare(psString);	
	   		BoundStatement bs = new BoundStatement(selectPS).bind(user1, user2, user3, end.getTime(), start.getTime());
	   		System.out.println(bs);
	   		
	    	// iterate through the result set and print the results on the console
	   		final ResultSetFuture queryFuture = session.executeAsync(bs);	
	   		ResultSet resultSet = queryFuture.getUninterruptibly();	 
	   		String user = "";
	   		long hits = 0;
	   		int count = 0;
	   		for (Row row : resultSet)
	   		{
	   			// update the map
	   			user = row.getString(0);
	   			hits = row.getLong(1);
	   			
	   			if (map.get(user) == null)
	   			{
	   				map.put(user, (int)hits);
	   				continue;
	   			}
	   			count = map.get(user);
	   			count += hits;
	   			map.put(user, count);
	   		}
	   		
	   		ValueComparator vc = new ValueComparator(map);
	   		TreeMap<String, Integer> treeMap = new TreeMap<String, Integer>(vc);
	   		treeMap.putAll(map);
	   		for (Map.Entry<String, Integer> entry : treeMap.entrySet()) {
	   		    System.out.println(entry.getKey() + ", " + entry.getValue());
	   		}
	   		cleanup();
   	}	
	
	public void cleanup() {
        session.shutdown();
        cluster.shutdown();
    }
	
	public static void main(String[] args) {
		UserEditsPerHour uph = new UserEditsPerHour();
		try {
			//uph.writeToDB();
			uph.testAllUserEditsPerHour("[23/Apr/2014:10]", "[03/May/2014:10]");
			//uph.usersBetweenHours("[03/Mar/2014:10]", "[13/Mar/2014:10]");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
