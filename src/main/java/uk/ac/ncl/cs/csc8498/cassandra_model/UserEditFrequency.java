package uk.ac.ncl.cs.csc8498.cassandra_model;

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
 * Find the frequency of all users who have edited a given number of times
 * @author b0354345
 *
 */
public class UserEditFrequency {
	private static Cluster cluster;
    private static Session session;
    
    public UserEditFrequency()
    { 
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
		
		 session.execute("CREATE TABLE IF NOT EXISTS edit_frequency_user (no_of_edits int, frequency counter, PRIMARY KEY (no_of_edits));");	
    }
    
    /**
     * Create a table with 'number of edits' column as a primary key, and a frequency column 
     * @throws InterruptedException
     */
    public void writeToDB() throws InterruptedException {
		String psString = "SELECT user, hits FROM total_edits_per_user;";

		final int maxOutstandingFutures = 4;
		final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(
				maxOutstandingFutures);
		// prepared statement for inserting records into the table
		final PreparedStatement updatePS = session
				.prepare("UPDATE edit_frequency_user SET frequency = frequency + ? WHERE no_of_edits = ?;");

		// iterate through the result set and print the results on the console
		final ResultSetFuture queryFuture = session.executeAsync(psString);
		ResultSet resultSet = queryFuture.getUninterruptibly();
		int count = 0;
		String user = "";
		int edits = 0;
		for (Row row : resultSet) {
			user = row.getString(0);
			if (user.toLowerCase().contains("bot"))  // eliminate bots
				continue;
			edits = (int)row.getLong(1);
			BoundStatement boundState = new BoundStatement(updatePS).bind(1L,
					edits);
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
     * For a given set of user's 'number of edits', return frequency for each 'number of edits'
     */
    public void testUserEditFrequency() 
   	{	
	    	String psString = "SELECT no_of_edits, frequency FROM edit_frequency_user WHERE no_of_edits in (?, ?, ?);";
	    	// prepared statement for querying the DB
	   		final PreparedStatement selectPS = session.prepare(psString);	
	   		BoundStatement bs = new BoundStatement(selectPS).bind(12, 100, 150);
	   		
	    	// iterate through the result set and print the results on the console
	   		final ResultSetFuture queryFuture = session.executeAsync(bs);	
	   		ResultSet resultSet = queryFuture.getUninterruptibly();	   		
	   		for (Row row : resultSet)
	   		{
	   			System.out.println(row.getInt(0) + " " + row.getLong(1));			
	   		}
	   		cleanup();
   	}
    
    /**
     * return all frequencies for each user's number of edits 
     */
    public void frequencyForEachNumbOfEdits()
    {
    	String psString = "SELECT * FROM edit_frequency_user";
    	final ResultSetFuture queryFuture = session.executeAsync(psString);
    	ResultSet resultSet = queryFuture.getUninterruptibly();
    	Map<String, Integer> map = new HashMap<String, Integer>();
    	String noOfEdits = "";
    	long fr = 0;
    	for (Row row : resultSet)
    	{
    		noOfEdits = ""+ row.getInt(0);
    		fr = row.getLong(1);   		
    		map.put(noOfEdits, (int)fr);
    	}
    	
    	ValueComparator vc = new ValueComparator(map);
   		TreeMap<String, Integer> treeMap = new TreeMap<String, Integer>(vc);
   		treeMap.putAll(map);
   		int value = 0;
   		String key = "";
   		for (Map.Entry<String, Integer> entry : treeMap.entrySet()) {
   			key = entry.getKey();
   			value = entry.getValue();
   		    System.out.println(key + " : " + value); 
   		}
   		cleanup();
    }
    
    public void cleanup() {
        session.shutdown();
        cluster.shutdown();
    }
    
    public static void main(String[] args)
    {
    	UserEditFrequency pg = new UserEditFrequency();
//    	try {
//			pg.writeToDB();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    	//pg.testUserEditFrequency();
    	pg.frequencyForEachNumbOfEdits();
    }

}
