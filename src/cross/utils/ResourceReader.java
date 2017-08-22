package cross.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class ResourceReader {
	
	static String line;
	static FileReader fr;
	
	public static ArrayList<String> reader(String path, ArrayList<String> list){
		
		File file = new File(path);
		try {
			fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			while((line = br.readLine()) != null){
				list.add(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return list;
	}
	
	
	
	public static HashSet<String> reader(String path, HashSet<String> hashSet){
		
		File file = new File(path);
		try {
			fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			while((line = br.readLine()) != null){
				hashSet.add(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return hashSet;
	}
}
