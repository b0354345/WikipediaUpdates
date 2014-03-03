package uk.ac.ncl.cs.csc8498.httpclient;

import java.sql.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class DateFormatter {
	
	public static String fomatDate(long milllis)
	{
		Date date = new Date(milllis);
		Calendar cal = new GregorianCalendar();
		cal.setTime(date);
		
		String year = new String("" + cal.get(Calendar.YEAR));
		String month = new String("" + (cal.get(Calendar.MONTH) + 1));
		String day = new String("" + cal.get(Calendar.DATE));
		String hour = new String("" + cal.get(Calendar.HOUR));
		String minute = new String("" + cal.get(Calendar.MINUTE));
		String second = new String("" + cal.get(Calendar.SECOND));
		
		String time = year + (month.length() == 2 ? month : "0" + month) + 
						(day.length() == 2 ? day : "0" + day) +
						(hour.length() == 2 ? hour : "0" + hour) +
						(minute.length() == 2 ? minute : "0" + hour) +
						(second.length() == 2 ? second : "0" + second);
		return time;
	}

}
