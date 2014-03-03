package uk.ac.ncl.cs.csc8498.httpclient;

import java.sql.Date;
import java.util.Timer;
import java.util.TimerTask;

public class TestClass extends TimerTask {
	long now;
	static int count = 0;
	public void run() {
		now = System.currentTimeMillis(); // initialize date
		Date date = new Date(now);
		System.out.println("Time is :" + date); // Display current time
		
	}

	public static void main(String args[]) throws InterruptedException {
		 
		Timer time = new Timer(); // Instantiate Timer Object
		TestClass st = new TestClass(); // Instantiate SheduledTask class
		time.schedule(st, 0, 1000); // Create Repetitively task for every 1 secs
		System.out.println("Execution in Main Thread...." + count++);
		if (count > 10)
			System.exit(0);
 
//		//for demo only.
//		for (int i = 0; i <= 5; i++) {
//			
//			Thread.sleep(2000);
//			if (i == 5) {
//				System.out.println("Application Terminates");
//				System.exit(0);
//			}
//		}
	}
}
