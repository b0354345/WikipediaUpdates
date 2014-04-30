package uk.ac.ncl.cs.csc8498.httpclient;

import java.io.IOException;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Timer;
import java.util.TimerTask;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import uk.ac.ncl.cs.csc8498.cassandra_model.PageEditsPerHour;

public class TestClass {
	private static Cluster cluster;
	private static Session session;
	
	public static void main(String args[]) {
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
		
		Date date = new Date(1396652400000L);
		String psSelect = "SELECT user FROM user_edit WHERE edit_time > " + date + " AND title = 'Talk:Malaysia Airlines Flight 370' LIMIT 100 ALLOW FILTERING;";
		final ResultSetFuture queryFuture = session.executeAsync(psSelect);
		ResultSet resultSet = queryFuture.getUninterruptibly();
		for (Row row : resultSet)
		{
			System.out.println(row.getString(0));
		}
	}
}
