package cross.prepare;

import java.io.File;

import cross.entity.ConfigData;
import cross.utils.LoadFile;

public class ReadyCheck {
	
	public int checkNecessaryFiles(String wrkingDir){
		
		File file = new File(wrkingDir);
		String[] fileList = file.list();
//		System.out.println("fileList.size:"+fileList.length);
		int count2250info = 0;
		int count2251info = 0;
		int count2250md5 = 0;
		int count2251md5 = 0;

		for(int i =0; i < fileList.length; i++){
			if(fileList[i].equals(ConfigData.fileInfo2250)){
				count2250info = 1;
			} else if(fileList[i].equals(ConfigData.fileInfoHdfs)){
				count2251info = 1;
			} else if(fileList[i].equals(ConfigData.fileMd5sum2250)){
				count2250md5 = 1;
			} else if(fileList[i].equals(ConfigData.fileMd5sumHdfs)){
				count2251md5 = 1;
			}
		}
		if((count2250info + count2251info + count2250md5 + count2251md5) == 4){
			return 1;
		}
		return 0;
	}
}
