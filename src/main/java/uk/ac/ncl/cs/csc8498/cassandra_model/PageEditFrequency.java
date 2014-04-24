package uk.ac.ncl.cs.csc8498.cassandra_model;

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

/**
 * Find the frequency of all the pages that have been edited a given number of times
 * @author b0354345
 *
 */
public class PageEditFrequency {
	private static Cluster cluster;
    private static Session session;
    
    public PageEditFrequency()
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
		
		 session.execute("CREATE TABLE IF NOT EXISTS edit_frequency_page (no_of_edits int, frequency counter, PRIMARY KEY (no_of_edits));");	
    }
    
    /**
     * Create a table with 'number of edits' column as a primary key, and a frequency column 
     * @throws InterruptedException
     */
    public void writeToDB() throws InterruptedException {
		String psString = "SELECT title, hits FROM edits_per_page;";

		final int maxOutstandingFutures = 4;
		final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(
				maxOutstandingFutures);
		// prepared statement for inserting records into the table
		final PreparedStatement updatePS = session
				.prepare("UPDATE edit_frequency_page SET frequency = frequency + ? WHERE no_of_edits = ?;");

		// iterate through the result set and print the results on the console
		final ResultSetFuture queryFuture = session.executeAsync(psString);
		ResultSet resultSet = queryFuture.getUninterruptibly();
		int count = 0;
		int edits = 0;
		String title = "";
		for (Row row : resultSet) {
			title = row.getString(0);
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
		cluster.shutdown();
	}
    
    /**
     * For a given set of 'number of edits', return frequency for each 'number of edits'
     */
    public void testPageEditFrequency() 
   	{	
	    	String psString = "SELECT no_of_edits, frequency FROM edit_frequency_page WHERE no_of_edits in (?, ?, ?);";
	    	// prepared statement for querying the DB
	   		final PreparedStatement selectPS = session.prepare(psString);	
	   		BoundStatement bs = new BoundStatement(selectPS).bind(5, 10, 15);
	   		
	    	// iterate through the result set and print the results on the console
	   		final ResultSetFuture queryFuture = session.executeAsync(bs);	
	   		ResultSet resultSet = queryFuture.getUninterruptibly();	   		
	   		for (Row row : resultSet)
	   		{
	   			System.out.println(row.getInt(0) + " " + row.getLong(1));			
	   		}
	   		cluster.shutdown();
   	}	
    
    public static void main(String[] args)
    {
    	PageEditFrequency pg = new PageEditFrequency();
    	try {
			pg.writeToDB();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	//pg.testPageEditFrequency();
    }
}
