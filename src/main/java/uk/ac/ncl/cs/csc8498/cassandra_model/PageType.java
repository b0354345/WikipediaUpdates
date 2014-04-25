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
 * For each wikipedia page type, find total number of edits
 * @author b0354345
 */
public class PageType {
	private static Cluster cluster;
    private static Session session;
    private static final String GENERAL = "general";
    private static final String USER = "user";
    private static final String USER_TALK = "user_talk";
    private static final String WIKIPEDIA = "wikipedia";
    private static final String FILE = "file";
    private static final String TEMPLATE = "template";
    private static final String TALK = "talk";
    
	
	 public PageType()
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
			 session.execute("CREATE TABLE IF NOT EXISTS page_type (type text, hits counter, PRIMARY KEY (type));");	
	    }
		 
		/**
		 * create table with 'type' column as the primary key, and count column to
		 * show number of times each page type has been edited.
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
					.prepare("UPDATE page_type SET hits = hits + ? WHERE type = ?;");

			String title = "";
			String type = "";
		
			// iterate through the result set and print the results on the console
			final ResultSetFuture queryFuture = session.executeAsync(psString);
			ResultSet resultSet = queryFuture.getUninterruptibly();
			int count = 0;
			for (Row row : resultSet) {
				title = row.getString(0);
				
				if (title.startsWith("Wikipedia"))
				{
					type = WIKIPEDIA;
				}else if (title.startsWith("User:"))
				{
					type = USER;
				}else if (title.startsWith("User talk:"))
				{
					type = USER_TALK;
				}else if (title.startsWith("File"))
				{
					type = FILE;
				}else if (title.startsWith("Talk"))
				{
					type = TALK;
				}else if (title.startsWith("Template"))
				{
					type =TEMPLATE;
				}else
				{
					type = GENERAL;
				}
				
				BoundStatement boundState = new BoundStatement(updatePS).bind(1L,
						type);
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
	     * test page types
	     */
	    public void testPageType()
	    {
	    	String psString = "SELECT title, hits FROM page_type WHERE type IN ('general', 'talk')";
	    	// iterate through the result set and print the results on the console
	   		final ResultSetFuture queryFuture = session.executeAsync(psString);	
	   		ResultSet resultSet = queryFuture.getUninterruptibly();	
	   		for (Row row : resultSet)
	   		{
	   			System.out.println(row.getString(0) + " : " + row.getLong(1));
	   		}
	   		cleanup();
	    }
	    
	    public void cleanup() {
	        session.shutdown();
	        cluster.shutdown();
	    }

	public static void main(String[] args) {
		
		PageType pt = new PageType();
		try {
			pt.writeToDB();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//pt.testPageType();
	}

}
