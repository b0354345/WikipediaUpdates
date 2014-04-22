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

public class TotalEditsPerPage {
	private static Cluster cluster;
    private static Session session;
    
    public TotalEditsPerPage()
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
		
		 session.execute("CREATE TABLE IF NOT EXISTS edits_per_page (title text, hits counter, PRIMARY KEY (title));");	
    }
    
    /**
     * 
     * @throws InterruptedException
     */
    public void writeToDB() throws InterruptedException {
		String psString = "SELECT title FROM user_edit;";

		final int maxOutstandingFutures = 4;
		final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(
				maxOutstandingFutures);
		// prepared statement for inserting records into the table
		final PreparedStatement updatePS = session
				.prepare("UPDATE edits_per_page SET hits = hits + ? WHERE title = ?;");

		String title = "";
	
		// iterate through the result set and print the results on the console
		final ResultSetFuture queryFuture = session.executeAsync(psString);
		ResultSet resultSet = queryFuture.getUninterruptibly();
		int count = 0;
		for (Row row : resultSet) {
			title = row.getString(0);
			BoundStatement boundState = new BoundStatement(updatePS).bind(1L,
					title);
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
	 *  This method sends a query to a DB to return total number of edits
	 *  for a given a set of users.
	 */
    public void totalAccessRead() 
   	{	
	    	// sample titles for the query
	    	String title1 = "Xunlei";
	    	String title2 = "Wikipedia:WikiProject Antarctica Highways";
	    	String title3 = "Talk:Spanish aircraft carrier Principe de Asturias";
	    
	    	String psString = "SELECT title, hits FROM edits_per_page WHERE title in (?, ?, ?);";
	    	// prepared statement for querying the DB
	   		final PreparedStatement selectPS = session.prepare(psString);	
	   		BoundStatement bs = new BoundStatement(selectPS).bind(title1, title2, title3);
	   		System.out.println(bs);
	   		
	    	// iterate through the result set and print the results on the console
	   		final ResultSetFuture queryFuture = session.executeAsync(bs);	
	   		ResultSet resultSet = queryFuture.getUninterruptibly();	   		
	   		for (Row row : resultSet)
	   		{
	   			System.out.println(row.getString(0) + " " + row.getLong(1));			
	   		}
	   		cluster.shutdown();
   	}	
}
