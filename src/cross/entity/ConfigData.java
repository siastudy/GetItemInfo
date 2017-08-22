package cross.entity;

import cross.utils.LoadFile;

public class ConfigData {
	
	
	public static String workdingDir2250;
	public static String workdingDirHdfs;
	public static String fileInfo2250;
	public static String fileInfoHdfs;
	public static String fileMd5sum2250;
	public static String fileMd5sumHdfs;
	public static String backupFolder2250;
	public static String backupFolderHdfs;
	public static String backupScriptName;
	public static String syncFolder;
	public static String clearDependFiles;
	public static String getFileInfoAndMd5FromHdfs;
	public static String getFileInfoAndMd5From2250;
	public static String syncMid;
	public static String cpToChangeGroup;
	public static String clearDependFileScript;
	public static String getFileMd5FromHdfs;
	public static String getFileInfoFromHdfs;
	public static String scriptFolder2250;
	
	public static void loadConfigFile(String configFileLocation){
		
		workdingDir2250 = LoadFile.getPropertyFileValue(configFileLocation, "WRKING_DIR_2250");
		workdingDirHdfs = LoadFile.getPropertyFileValue(configFileLocation, "WRKING_DIR_HDFS");
		fileInfo2250 = LoadFile.getPropertyFileValue(configFileLocation, "FILE_INFO_2250");
		fileInfoHdfs = LoadFile.getPropertyFileValue(configFileLocation, "FILE_INFO_HDFS");
		fileMd5sum2250 = LoadFile.getPropertyFileValue(configFileLocation, "FILE_MD5SUM_2250");
		fileMd5sumHdfs = LoadFile.getPropertyFileValue(configFileLocation, "FILE_MD5SUM_HDFS");
		backupFolder2250 = LoadFile.getPropertyFileValue(configFileLocation, "BACKUP_FOLDER_2250");
		backupFolderHdfs = LoadFile.getPropertyFileValue(configFileLocation, "BACKUP_FOLDER_HDFS");
		backupScriptName = LoadFile.getPropertyFileValue(configFileLocation, "BACKUP_FOLDER_NAME");
		syncFolder = LoadFile.getPropertyFileValue(configFileLocation, "BACKUP_SCRIPT_NAME");
		clearDependFiles = LoadFile.getPropertyFileValue(configFileLocation, "CLEAR_DEPEND_FILES");
		getFileMd5FromHdfs = LoadFile.getPropertyFileValue(configFileLocation, "GET_FILE_MD5_FROM_HDFS");
		getFileInfoFromHdfs = LoadFile.getPropertyFileValue(configFileLocation, "GET_FILE_INFO_FROM_HDFS");
		getFileInfoAndMd5From2250 = LoadFile.getPropertyFileValue(configFileLocation, "GET_FILE_INFO_AND_MD5_FROM_2250");
		syncMid = LoadFile.getPropertyFileValue(configFileLocation, "SYNC_MID");
		cpToChangeGroup = LoadFile.getPropertyFileValue(configFileLocation, "CP_TO_CHANGE_GROUP");
		clearDependFileScript = LoadFile.getPropertyFileValue(configFileLocation, "CLEAR_DEPEND_FILES_SCRIPT");
		scriptFolder2250 = LoadFile.getPropertyFileValue(configFileLocation, "SCRIPT_FOLDER_2250");

	}
}
