package uk.ac.ncl.cs.csc8498.httpclient;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;



public class JDOMXMLParser {
	private static long count;

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
        Element queryNode = root.getChild("query");
        Element rcNode = queryNode.getChild("recentchanges");
        
        List<Element> rcList = rcNode.getChildren();
        
      
        //Iterating over all childs in XML
        Iterator<Element> itr = rcList.iterator();
        while(itr.hasNext()){
        	Element rc = itr.next();
            //reading attribute from Element using JDOM
            System.out.println("User: " + rc.getAttributeValue("user") + " Title: " + rc.getAttributeValue("title") + " Timestamp: " + rc.getAttributeValue("timestamp"));  
            count++;
        }        
    }
    
    public static long getCount()
    {
    	return count;
    }
}