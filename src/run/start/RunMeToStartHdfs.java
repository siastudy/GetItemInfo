package run.start;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import cross.compare.CompareAndRun;
import cross.entity.ConfigData;
import cross.entity.EntityData;
import cross.prepare.ReadAndProcessFile;
import cross.prepare.ReadyCheck;
import cross.utils.LinuxShell;
import cross.utils.LoadFile;

public class RunMeToStartHdfs {
	
	private static Logger logger = Logger.getLogger(RunMeToStartHdfs.class);
	
	public static ReadAndProcessFile rp = new ReadAndProcessFile();
	
	public static ArrayList<String> arrayList = new ArrayList<String>();
	public static ArrayList<String> fileInfoList = new ArrayList<String>();
	public static ArrayList<String> md5sumList = new ArrayList<String>();
	
	public static void main(String[] args){
		logger.info("entered into main");
		getPrepared(args[0], args[1]);
		
		collectAndLoadFingerprintFiles();
		
		doCompareAndCp();
		
		clearContainers();
	}
	
	
	public static int getPrepared(String configFileLocation, String fullPath){
		//clear former fingerprint files
		logger.info("getting prepared");
		String deleteCommand = "cd " + ConfigData.workdingDir2250 + " && sh " + ConfigData.clearDependFileScript;
		logger.info("deleting command:"+deleteCommand);
			LinuxShell.runShell(deleteCommand);
		
		//load config data
		ConfigData.loadConfigFile(configFileLocation);
		logger.info("config file loaded");
		
		
		LoadFile.readFileIntoListAndMap(fullPath);
		logger.info("load hadoop file and path into map completed");
		return 1;
	}
	
	public static int collectAndLoadFingerprintFiles(){
		
		String get2250FingerCmd;
		
		ReadyCheck rc = new ReadyCheck();
		//run shell to get fingerprint files for HDFS
			logger.info("about to collect hdfs finger prints");
			logger.info("hdfs: getfileinfocommand:"+"cd "+ConfigData.workdingDirHdfs+" "+"&&"+" "+"sh "+ConfigData.getFileInfoFromHdfs);
			LinuxShell.runShell("cd "+ConfigData.workdingDirHdfs+" "+"&&"+" "+"sh "+ConfigData.getFileInfoFromHdfs);
			logger.info("hdfs: getfilemDd5command:"+"cd "+ConfigData.workdingDirHdfs+" "+"&&"+" "+"sh "+ConfigData.getFileMd5FromHdfs);
			LinuxShell.runShell("cd "+ConfigData.workdingDirHdfs+" "+"&&"+" "+"sh "+ConfigData.getFileMd5FromHdfs);
			logger.info("about to collect 2250 finger prints");
			//get fingerprint files for 2250
			//SCRIPT_FOLDER=$1   INFO_FILE_NAME=$2   MD5_FILE_NAME=$3   TO_FOLDER_NAME=$4
			get2250FingerCmd = "cd "+ConfigData.workdingDir2250+" "+"&&"+" "+"sh "+ConfigData.getFileInfoAndMd5From2250+" "+ConfigData.scriptFolder2250+" "+ConfigData.fileInfo2250+" "+ConfigData.fileMd5sum2250+" "+ConfigData.workdingDir2250;
			logger.info("get2250FingerCmd:"+get2250FingerCmd);
			LinuxShell.runShell(get2250FingerCmd);

			for (int depCount = 0; depCount < 30; depCount++) {
			if(rc.checkNecessaryFiles(ConfigData.workdingDir2250) == 1){
				break;
			} else if(depCount == 20){
				System.exit(0);
			}else{
				try {
					logger.info("depend fingerprint files not reay");
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		//load fingerprint files into container
		logger.info("loading 2250 fingerprint files into list");
		fileInfoList = rp.readFileIntoList(ConfigData.workdingDir2250+"/"+ConfigData.fileInfo2250, fileInfoList);
		md5sumList = rp.readFileIntoList(ConfigData.workdingDir2250+"/"+ConfigData.fileMd5sum2250, md5sumList);
		
		logger.info("2250   fileInfoList.size:"+fileInfoList.size());
		logger.info("2250   md5sumList.size:"+ md5sumList.size());
		
		rp.getFilenameSizeTimeAndSaveToMap(fileInfoList, "2250");
		rp.getMd5sumAndSaveToMap(md5sumList, "2250");
		
		
		
		fileInfoList.clear();
		md5sumList.clear();
		logger.info("loading hdfs fingerprint files into list");
		fileInfoList = rp.readFileIntoList(ConfigData.workdingDir2250+"/"+ConfigData.fileInfoHdfs, fileInfoList);
		md5sumList = rp.readFileIntoList(ConfigData.workdingDir2250+"/"+ConfigData.fileMd5sumHdfs, md5sumList);
		logger.info("hdfs   fileInfoList.size:"+fileInfoList.size());
		logger.info("hdfs   md5sumList.size:"+ md5sumList.size());
		
		rp.getFilenameSizeTimeAndSaveToMap(fileInfoList, "hdfs");
		logger.info("hdfs: fileinfo list has been saved into map");
		rp.getMd5sumAndSaveToMap(md5sumList, "hdfs");
		return 1;
	}
	
	
	public static void doCompareAndCp(){
		logger.info("do compare start....");
		CompareAndRun cr = new CompareAndRun();
		// do compare
		cr.compareAndCopy();
	}
	
	public static void clearContainers(){
		EntityData.hashSet.clear();
		EntityData.fileInfoMap.clear();
		EntityData.map2250.clear();
		EntityData.mapHdfs.clear();
		EntityData.fileInfoList2250.clear();
		EntityData.fileMd5List2250.clear();
		EntityData.fileInfoListHdfs.clear();
		EntityData.fileMd5ListHdfs.clear();
		EntityData.fileAndPathMap.clear();
	}
}
