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
 * For a given page title, find how many times that page has been edited.
 * @author b0354345
 *
 */
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
     * create table with 'title' column as the primary key, and count column to show 
     * how many times each page has been edited.
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

			if (title.startsWith("User") || title.startsWith("Wikipedia") || title.startsWith("File") 
					|| title.startsWith("Talk")|| title.startsWith("Template"))
				continue;
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
		cleanup();
	}
    
    /**
	 *  This method sends a query to a DB to return total number of edits
	 *  for a given a set of pages.
	 */
    public void testTotalEditsPerPage() 
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
	   		cleanup();
   	}
    

    /**
     * Select top 100 most edited pages
     * @param args
     */
    public void selectMostDicussedPages()
    {
    	String psString = "SELECT title, hits FROM edits_per_page;";
    	
    	// store the titles and hits for each titled in the map
    	Map<String, Integer> map = new HashMap<String, Integer>();
    	// prepared statement for querying the DB
   		final PreparedStatement selectPS = session.prepare(psString);	
   		// iterate through the result set and print the results on the console
   		final ResultSetFuture queryFuture = session.executeAsync(psString);	
   		ResultSet resultSet = queryFuture.getUninterruptibly();	
   		String title = "";
   		long hit = 0;
   		for (Row row : resultSet)
   		{
   			// update the map
   			title = row.getString(0);
   			hit = row.getLong(1);
   			if (map.get(title) != null)
   			{
   				int count = map.get(title);
   				count += hit;
   				map.put(title, count);
   			}
   			else
   			{
   				map.put(title, (int)hit);
   			}
   		}
   		
   		ValueComparator vc = new ValueComparator(map);
   		TreeMap<String, Integer> treeMap = new TreeMap<String, Integer>(vc);
   		treeMap.putAll(map);
   		int count = 0;
   		for (Map.Entry<String, Integer> entry : treeMap.entrySet()) {
   		    System.out.println(entry.getKey() + ", " + entry.getValue());
   		    count++;
   		    if (count >= 100)
   		    	break;
   		}
   		cleanup();
    }
    
    public void cleanup() {
        session.shutdown();
        cluster.shutdown();
    }
    
    public static void main(String[] args)
    {
    	TotalEditsPerPage pg = new TotalEditsPerPage();
//    	try {
//			pg.writeToDB();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    	//pg.testTotalEditsPerPage();
    	pg.selectMostDicussedPages();
    }
}
