package uk.ac.ncl.cs.csc8498.cassandra_model;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

public class CassandraCluster {
	
	private static Cluster cluster;
	private static Session session;
	private static DateFormat dateFormat;
	
	
	/**
	 * Public constructor for creating UserSession object
	 */
	public CassandraCluster() {
		cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build();

		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");		

		final int numberOfConnections = 1;
		PoolingOptions poolingOptions = cluster.getConfiguration()
				.getPoolingOptions();
		poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL,
				numberOfConnections);
		poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL,
				numberOfConnections);
		poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE,
				numberOfConnections);
		poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE,
				numberOfConnections);

		final Session bootstrapSession = cluster.connect();
		bootstrapSession
				.execute("CREATE KEYSPACE IF NOT EXISTS wikiproject WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
		bootstrapSession.shutdown();

		session = cluster.connect("wikiproject");

		session.execute("CREATE TABLE IF NOT EXISTS user_edit (user text, userId text, edit_time timestamp, pageId text, title text, "
				+ "recentChangeId text, oldRevId text, newRevId text, type text, PRIMARY KEY ((user, edit_time, title), recentChangeId));");				
	}

	/**
	 * @throws ParseException 
	 * @throws InterruptedException 
	 * 
	 */
	public void writeWikiResults(String user, String userId, String time, String pageId, String title, String recentChangeId, String oldRevId, String newRevId, String type) throws ParseException, InterruptedException
	{
		
		final int maxOutstandingFutures = 4;
		final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<ResultSetFuture>(maxOutstandingFutures);
		// prepared statement for inserting records into the table
		final PreparedStatement insertPS = session.prepare("INSERT INTO user_edit (user, userId, edit_time, pageId, title, recentChangeId, oldRevId, newRevId, type) "
											+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");	
		final BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
		
		Date date = processDate(time);
		//System.out.println("**** " + date);
	  
        int itemsPerBatch = 0;
        while (itemsPerBatch < 100) {			
			batchStatement.add(new BoundStatement(insertPS).bind(user, userId, date, pageId, title, recentChangeId, oldRevId, newRevId, type));
			itemsPerBatch++;						
		}
		outstandingFutures.put(session.executeAsync(batchStatement));

		if (outstandingFutures.remainingCapacity() == 0) {
			ResultSetFuture resultSetFuture = outstandingFutures.take();
			resultSetFuture.getUninterruptibly();
		}
	}

	private Date processDate(String time) throws ParseException {
		String timestamp = time.replace('T', ' ').replace('-', '/');
		timestamp = timestamp.substring(0, time.length() - 1);
		//System.out.println("**** " + timestamp);
		Date date = dateFormat.parse(timestamp);
		return date;
	}
  
    public void cleanup() {
        session.shutdown();
        cluster.shutdown();
    }

}
