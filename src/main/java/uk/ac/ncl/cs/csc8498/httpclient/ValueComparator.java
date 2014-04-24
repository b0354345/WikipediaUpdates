package uk.ac.ncl.cs.csc8498.httpclient;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class ValueComparator implements Comparator<String> {

    Map<String, Integer> base;
    public ValueComparator(Map<String, Integer> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
    
    
    public static void main(String[] args) {

        HashMap<String,Integer> map = new HashMap<String,Integer>();
        map.put("A",500);
        map.put("B",67);
        map.put("C",5);
        map.put("D",1);
        System.out.println("unsorted map: "+map); 
       ValueComparator bvc =  new ValueComparator(map);
       System.out.println("sorted map: "+bvc); 
        TreeMap<String,Integer> sorted_map = new TreeMap<String,Integer>(bvc);
       sorted_map.putAll(map);
       System.out.println("results: "+sorted_map);
    }
}
