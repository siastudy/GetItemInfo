package cross.prepare;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import cross.entity.EntityData;
import cross.utils.ResourceReader;

public class ReadAndProcessFile {
	
	private Logger logger = Logger.getLogger(ReadAndProcessFile.class);
	
	public ArrayList<String> readFileIntoList(String filePath, ArrayList<String> arrayList){
		return ResourceReader.reader(filePath, arrayList);
	}
	
	/**
	 * 把arrayList中的关键字提取出来, 按照固定的格式,保存到map中
	 * 
	 * */
	public void getFilenameSizeTimeAndSaveToMap(ArrayList<String> arrayList, String whichServer){
		
		String keyLine;
		
		String fileName;
		String archiveTime;
		String fileSize;
		
		int count2250map=0;
		int count2251map=0;
		
		
		for(int i = 0; i < arrayList.size(); i++){
			keyLine = arrayList.get(i);
			String[] lineElements = keyLine.split(" +");
//			for(int m = 0; m < lineElements.length; m++){
//				logger.info(m+"==="+lineElements[m]);
//			}
			fileName = lineElements[8];
			archiveTime = lineElements[5]+" "+lineElements[6];
			fileSize = lineElements[4];
			//System.out.println(fileName+",filesize:"+fileSize);
			
			//save each file name into one set:
			EntityData.hashSet.add(fileName);
			
			switch(whichServer){
			case "2250":
				count2250map++;
				EntityData.map2250.put(fileName+"---archiveTime", archiveTime);
				EntityData.map2250.put(fileName+"---fileSize", fileSize);
				//System.out.println("filesize:"+fileSize+" has been put into 2250");
				break;
			case "hdfs":
				EntityData.mapHdfs.put(fileName+"---archiveTime", archiveTime);
				EntityData.mapHdfs.put(fileName+"---fileSize", fileSize);
				break;
			}
		}
	}
	
	public void getMd5sumAndSaveToMap(ArrayList<String> arrayList, String whichServer){
		
		String md5sum;
		String keyLine;
		String fileName;
		
		int count2250mapmd5 = 0;
		int count2251mapmd5 = 0;
		
		for(int i = 0; i < arrayList.size(); i++){
			keyLine = arrayList.get(i);
			
			String[] lineElements = keyLine.split(" +");
			
			fileName = lineElements[1];
			md5sum = lineElements[0];
			
			switch(whichServer){
			case "2250":
				count2250mapmd5++;
				EntityData.map2250.put(fileName+"---md5sum", md5sum);
				break;
			case "hdfs":
				EntityData.mapHdfs.put(fileName+"---md5sum", md5sum);
				break;
			}
		}
		
		logger.info("map2250.size:"+EntityData.map2250.size());
		logger.info("mapHdfs.size:"+EntityData.mapHdfs.size());
	}
}
