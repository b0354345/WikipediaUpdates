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
 * For each wikipedia edit type, find total number of edits
 * @author b0354345
 */
public class EditType {

	private static Cluster cluster;
	private static Session session;

	public EditType() {
		cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build();
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
		session.execute("CREATE TABLE IF NOT EXISTS edit_type (type text, hits counter, PRIMARY KEY (type));");
	}

	/**
	 * create table with 'type' column as the primary key, and count column to
	 * show number of times each edit type has been edited.
	 * 
	 * @throws InterruptedException
	 */
	public void writeToDB() throws InterruptedException {
		String psString = "SELECT type FROM user_edit;";

		final int maxOutstandingFutures = 4;
		final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(
				maxOutstandingFutures);
		// prepared statement for inserting records into the table
		final PreparedStatement updatePS = session
				.prepare("UPDATE edit_type SET hits = hits + ? WHERE type = ?;");

		String type = "";

		// iterate through the result set and print the results on the console
		final ResultSetFuture queryFuture = session.executeAsync(psString);
		ResultSet resultSet = queryFuture.getUninterruptibly();
		int count = 0;
		for (Row row : resultSet) {
			type = row.getString(0);
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
	 * Query the 'edit_type' table to test the implementation
	 */
	public void testEditType() {
		String psString = "SELECT type, hits FROM edit_type WHERE type IN ('edit', 'new')";
		ResultSet result = session.execute(psString);
		for (Row row : result) {
			System.out.println(row.getString(0) + " : " + row.getLong(1));
		}
		cleanup();
	}

	
	public void cleanup() {
		session.shutdown();
		cluster.shutdown();
	}

	public static void main(String[] args) {
		EditType et = new EditType();
//		try {
//			et.writeToDB();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		et.testEditType();

	}

}
