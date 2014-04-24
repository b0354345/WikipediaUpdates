package uk.ac.ncl.cs.csc8498.cassandra_model;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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
 * 
 * @author b0354345
 *
 */
public class MapEntry {
	private static Cluster cluster;
    private static Session session;
    private static DateFormat dateFormat;
    final PreparedStatement insertPSNew;
    final PreparedStatement insertPSUpdate;
    final PreparedStatement selectPS;
   
    public MapEntry()
    { 
    	 cluster = new Cluster.Builder().addContactPoint("127.0.0.1").build(); 
    	 dateFormat = new SimpleDateFormat("yyyy/MM/dd");	
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
		
		 session.execute("CREATE TABLE IF NOT EXISTS map_test (day timestamp, mymap map<text, int>, PRIMARY KEY (day));");	
		 insertPSNew = session.prepare("INSERT INTO map_test (day, mymap) VALUES (?, ?)");
		 insertPSUpdate = session.prepare("UPDATE wikiproject.map_test SET mymap = ? WHERE day = ?");
		 selectPS = session.prepare("SELECT mymap FROM wikiproject.map_test WHERE day = ?");
    }
    
	public void writeToDB() throws ParseException {
		String psString = "SELECT title, edit_time FROM user_edit;";

		// iterate through the result set and print the results on the console
		final ResultSetFuture queryFuture = session.executeAsync(psString);
		ResultSet resultSet = queryFuture.getUninterruptibly();
		int count = 0;
		String title = "";
		String time = "";
		for (Row row : resultSet) {
			title = row.getString(0);
			time = dateFormat.format(row.getDate(1));
			if (title.startsWith("User") || title.startsWith("Wikipedia") || title.startsWith("File") || title.startsWith("Template"))
			{
				continue;
			}
			updateEntry(time, title);
			System.out.println(count++);
		}
		session.shutdown();
	}
    
    public void writeNewEntry(String time, String key) throws ParseException
    {
    	Map<String, Integer> map = new HashMap<String, Integer>();
    	map.put(key,  1);
    	
    	Date day = dateFormat.parse(time);
    	//final PreparedStatement insertPS = session.prepare("INSERT INTO map_test (day, mymap) VALUES (?, ?)");
		ResultSet result = session.execute(new BoundStatement(insertPSNew).bind(day, map));
    }
    
    public void updateEntry(String time, String key) throws ParseException
    {
    	Date day = dateFormat.parse(time);
    	//final PreparedStatement selectPS = session.prepare("SELECT mymap FROM wikiproject.map_test WHERE day = ?");
    	ResultSet selectResult = session.execute(new BoundStatement(selectPS).bind(day));
    	Row row = selectResult.one();
    	if (row == null)
    	{
    		writeNewEntry(time, key);
    	}
    	else
    	{	
    		HashMap<String, Integer> rMap = new HashMap<String, Integer>(row.getMap(0, String.class, Integer.class));
    		Integer value = rMap.get(key);
    		if (value == null)
    		 	rMap.put(key,  1);
    		else
    		 	rMap.put(key,  value + 1);
    		
    		Map<String, Integer> sorted = sortByValues(rMap);
        	//final PreparedStatement insertPS = session.prepare("UPDATE wikiproject.map_test SET mymap = ? WHERE day = ?");	        	
    		ResultSet result = session.execute(new BoundStatement(insertPSUpdate).bind(sorted, day));	
    	}
    	// create new map	
    }
    
    
    public <K, V extends Comparable<V>> Map<K, V> sortByValues(final Map<K, V> map) {
        Comparator<K> valueComparator =  new Comparator<K>() {
            public int compare(K k1, K k2) {
                int compare = map.get(k2).compareTo(map.get(k1));
                if (compare == 0) return 1;
                else return compare;
            }
        };
        Map<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
        sortedByValues.putAll(map);
        return sortedByValues;
    }
   
    
    public static void main(String[] args)
    {  	
    	MapEntry map = new MapEntry();	
    	try {
    		map.writeToDB();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
