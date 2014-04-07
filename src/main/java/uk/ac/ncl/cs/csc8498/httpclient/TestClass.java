package uk.ac.ncl.cs.csc8498.httpclient;

import java.io.IOException;
import java.sql.Date;
import java.text.ParseException;
import java.util.Timer;
import java.util.TimerTask;

public class TestClass extends TimerTask {
	static long now;
	static int count = 0;
	public void run() {
		now = System.currentTimeMillis(); // initialize date
		Date date = new Date(now);
		System.out.println("Time is :" + date); // Display current time
		
	}

	public static void main(String args[]) throws InterruptedException {
		TotalEditsPerPage ed = new TotalEditsPerPage();
		try {
			ed.writeToDB();
			//ed.totalAccessRead("[04/Apr/2014:10]", "[01/Mar/2014:10]");
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
