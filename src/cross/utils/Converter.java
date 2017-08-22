package cross.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.swing.text.SimpleAttributeSet;

public class Converter {
	
	public static DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	
	public static long time1;
	public static long time2;
	public static long time3;
	
	public static long maxValue;
	
	
	/*
	 * convert String to date
	 * */
	public static Date converStringToDate(String dateString){
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			Date date;
			try {
				date = df.parse(dateString);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return null;
	}
	
	
	/*
	 * compare 2 string form date for the latest one
	 * */
	public static String compareStringASDate(String dateString1, String dateString2){
		
		try {
			time1 = df.parse(dateString1).getTime();
			time2 = df.parse(dateString2).getTime();
			if(time1 > time2){
				return dateString1;
			} else if(time1 < time2) {
				return dateString2;
			} else {
				return "=";
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
 		return null;
	}
	
	public static String compareStringAsDate(String dateString1, String dateString2, String dateString3){
		
		try{
			time1 = df.parse(dateString1).getTime();
			time2 = df.parse(dateString2).getTime();
			time3 = df.parse(dateString3).getTime();
			
			maxValue = Math.max(time1, Math.max(time2, time3));
			
			if(time1 == maxValue){
				return dateString1;
			} else if (time2 == maxValue){
				return dateString2;
			} else {
				return dateString3;
			}
			
		} catch (ParseException e){
			e.printStackTrace();
		}
		
		
		
		
		return null;
	}
	
	
	/*
	 * cut : 2017-05-18 07:06:59 ----> 2017-05-18 07:06:59
	 * */
	public static String cutStringToRegularFormat(String str){
		
		return str.split("\\.")[0];
		
	}
	
}
