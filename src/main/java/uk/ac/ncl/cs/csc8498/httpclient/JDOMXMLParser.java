package uk.ac.ncl.cs.csc8498.httpclient;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.List;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import uk.ac.ncl.cs.csc8498.cassandra_model.CassandraCluster;



public class JDOMXMLParser {
	private static long count;
	public static CassandraCluster cluster = new CassandraCluster();
    public static void parseXML(String filename) {
    	//creating JDOM SAX parser
        SAXBuilder builder = new SAXBuilder();
      
        //reading XML document
        Document xml = null;
        try {
                xml = builder.build(new File(filename));
        } catch (JDOMException e) {
                e.printStackTrace();
        } catch (IOException e) {
                e.printStackTrace();
        }
      
        //getting root element from XML document
        Element root = xml.getRootElement();
        if (root != null)
        {
	        Element queryNode = root.getChild("query");
	        if (queryNode != null)
	        {
		        Element rcNode = queryNode.getChild("recentchanges");
		        
		        List<Element> rcList = rcNode.getChildren();
		        
		       
		      
		        //Iterating over all childs in XML
		        Iterator<Element> itr = rcList.iterator();
		        while(itr.hasNext()){
		        	Element rc = itr.next();
		        	String user = rc.getAttributeValue("user");
		        	String userId = rc.getAttributeValue("userid");
		        	String pageId = rc.getAttributeValue("pageid");
		        	String title = rc.getAttributeValue("title");
		        	String timestamp = rc.getAttributeValue("timestamp");
		        	String comments = rc.getAttributeValue("comment");
		        	String type = rc.getAttributeValue("type");
		        	String oldRevId = rc.getAttributeValue("old_revid");
		        	String newRevId = rc.getAttributeValue("revid");
		        	String recentChangeId = rc.getAttributeValue("rcid");
		        	
		//        	 //reading attribute from Element using JDOM
		//            System.out.println("User: " + user + " | User_ID: " + userId + " | Page_ID: " + pageId + 
		//            		" | Title: " + title + " | Timestamp: " + timestamp + " | Recent_Change_ID: " + recentChangeId + " | Old_Rev_ID: " + oldRevId + 
		//            		" | New_Rev_ID: " + newRevId + " | Type: " + type);  
		       	
		        	try {
						cluster.writeWikiResults(user, userId, timestamp, pageId, title, recentChangeId, oldRevId, newRevId, type);
						 System.out.println(count++);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		        }
	        }
        }        
    }
    
    public static long getCount()
    {
    	return count;
    }
}