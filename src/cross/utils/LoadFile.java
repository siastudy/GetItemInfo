package cross.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.apache.log4j.Logger;

import cross.entity.EntityData;

public class LoadFile {
	
	public static FileInputStream fis;
	public static File file;
	public static Properties properties;
	public static String propertyValue;
	private static Logger logger = Logger.getLogger(LoadFile.class);
	
	public static String getPropertyFileValue(String pathname, String key){
		file = new File(pathname);
		try {
			fis = new FileInputStream(file);
			properties = new Properties();
			properties.load(fis);
			propertyValue = properties.getProperty(key);
			fis.close();
			return propertyValue;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "completed";
	}
	
	
	public static String readFileIntoListAndMap(String fullPath){
		file = new File(fullPath);
		ArrayList<String> fileAndPathList = new ArrayList<String>();
		String[] configLine;
		String line;
		try {
			fis= new FileInputStream(file);
			Reader reader = new FileReader(file);
			BufferedReader br = new BufferedReader(reader);
			while(( line = br.readLine())!=null){
				fileAndPathList.add(line);
			}
			fis.close();
		logger.info("hadoop file and location config has read into list, size:"+fileAndPathList.size());
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		for(int i = 0; i < fileAndPathList.size(); i++){
			configLine = fileAndPathList.get(i).split("===");
			//file name as key, path as value
			EntityData.fileAndPathMap.put(configLine[1], configLine[0]);
		}
		logger.info("convert config lines into map succeeded. key: filename, value: filepath, map.size:"+EntityData.fileAndPathMap.size());
		
		return "completed";
	}
}
