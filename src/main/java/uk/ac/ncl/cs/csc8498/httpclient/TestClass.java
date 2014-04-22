package uk.ac.ncl.cs.csc8498.httpclient;

import java.io.IOException;
import java.sql.Date;
import java.text.ParseException;
import java.util.Timer;
import java.util.TimerTask;

import uk.ac.ncl.cs.csc8498.cassandra_model.PageEditsPerHour;

public class TestClass extends TimerTask {
	static long now;
	static int count = 0;
	public void run() {
		now = System.currentTimeMillis(); // initialize date
		Date date = new Date(now);
		System.out.println("Time is :" + date); // Display current time
		
	}

	public static void main(String args[]) {
		
		PageEditsPerHour edu = new PageEditsPerHour();
		
<<<<<<< HEAD
		//edu.writeToDB();
		//edu.totalAccessRead();
=======
		TotalEditsPerPage edu = new TotalEditsPerPage();
		edu.writeToDB();
//		edu.totalAccessRead();
>>>>>>> 3d157a4bf1bf8c636ddcbb332201a43352716214
//		TotalEditsPerPage ed = new TotalEditsPerPage();
		try {
			//edu.writeToDB();
			//edu.pageEditsBetweenHours("[04/Apr/2014:10]", "[01/Mar/2014:10]");
			edu.allEditsBetweenHours("[10/Apr/2014:10]", "[09/Apr/2014:10]");
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
