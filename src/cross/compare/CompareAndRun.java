package cross.compare;

import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import cross.entity.ConfigData;
import cross.entity.EntityData;
import cross.utils.Converter;
import cross.utils.LinuxShell;

public class CompareAndRun {
	
	private static Logger logger = Logger.getLogger(CompareAndRun.class);

	Set<String> keyset2250 = EntityData.map2250.keySet();
	Set<String> keysetHdfs = EntityData.mapHdfs.keySet();

	Iterator<String> iter = EntityData.hashSet.iterator();

	String fileName;
	int fileExist2250 = 0;
	int fileExistHdfs = 0;

	int fileSize2250;
	int fileSizeHdfs;

	boolean boo2250;
	boolean booHdfs;
	String filePathHdfs;
	String copyRes;

	public void compareAndCopy() {
		while (iter.hasNext()) {
			fileName = iter.next();
			filePathHdfs = EntityData.fileAndPathMap.get(fileName);
			if(filePathHdfs==null){
				logger.error("===============file:"+fileName+"is a new file to HDFS======not copied to hdfs======================");
			} else {
				if (validateFile(fileName, keyset2250.iterator()) == 1 && validateFile(fileName, keysetHdfs.iterator()) == 1) {
					fileExist2250 = checkFileExistAfterValidation(keyset2250, fileName);
					fileExistHdfs = checkFileExistAfterValidation(keysetHdfs, fileName);

					if (fileExist2250 == 1 && fileExistHdfs != 1) {
						logger.info(fileName+":fileExist2250 == 1 && fileExistHdfs != 1");
						// cp file from 2250--->hdfs
						logger.info("command44:"+"cd "+ConfigData.workdingDirHdfs+" && "+"sh hadoop-copy.sh "+filePathHdfs+" "+fileName+"2250-hadoop");
						copyRes = LinuxShell.runShell("cd "+ConfigData.workdingDirHdfs+" && "+"sh hadoop-copy.sh "+filePathHdfs+" "+fileName+"2250-hadoop").replaceAll("\r|\n", "");
						if(copyRes.equals("completed")){
							logger.info("copy file:"+fileName+"from 2250 to hdfs succeed!!");
						} else {
							logger.error("error!!! file:"+fileName+"cp from 2250--hdfs failed!");
						}
					} else if (fileExist2250 != 1 && fileExistHdfs == 1) {
						logger.info(fileName+":fileExist2250 != 1 && fileExistHdfs == 1");
						// cp file from hdfs to 2250
						logger.info("command 50"+"cd "+ConfigData.workdingDirHdfs+" && "+"sh hadoop-copy.sh "+filePathHdfs+" "+fileName+"hadoop-2250");
						copyRes = LinuxShell.runShell("cd "+ConfigData.workdingDirHdfs+" && "+"sh hadoop-copy.sh "+filePathHdfs+" "+fileName+"hadoop-2250").replaceAll("\r|\n", "");
						if(copyRes.equals("completed")){
							logger.info("copy file:"+fileName+"from hdfs to 2250 succeed!!");
						} else {
							logger.error("error!!! file:"+fileName+"cp from hdfs--2250 failed!");
						}
					} else if (fileExist2250 == 1 && fileExistHdfs == 1) {
						logger.info(fileName+":fileExist2250 == 1 && fileExistHdfs == 1");
						compareForLatestFile(fileName);

					} else {
						// TODO, log
					}
				}
			}
		}
	}

	public void compareForLatestFile(String fileName) {
		try {
			fileSize2250 = Integer.parseInt(EntityData.map2250.get(fileName + "---fileSize"));
			fileSizeHdfs = Integer.parseInt(EntityData.mapHdfs.get(fileName + "---fileSize"));
		} catch (NumberFormatException e) {
			System.out.println(">>>>>>>>filesize format is not right, null??<<<" + fileName + "<<<<");
		}

		boo2250 = (fileSize2250 >= 2);
		booHdfs = (fileSizeHdfs >= 2);

		String archiveTime2250 = EntityData.map2250.get(fileName + "---archiveTime");
		String archiveTimeHdfs = EntityData.mapHdfs.get(fileName + "---archiveTime");

		String md5sum2250 = EntityData.map2250.get(fileName + "---md5sum");
		String md5sumHdfs = EntityData.mapHdfs.get(fileName + "---md5sum");

		if (boo2250 && booHdfs) {
			if (!(md5sum2250.equals(md5sumHdfs)))
			{
				switch (compare2Time(archiveTime2250, archiveTimeHdfs)) {
				case 1:
					// 2250 > hdfs, 2250----> hdfs
					// backup files in hdfs
					logger.info(fileName+":archiveTime2250 > archiveTimeHdfs");
					// put file from 2250 ----> hdfs
					logger.info("command90"+"cd "+ConfigData.workdingDirHdfs+" && sh hadoop-copy.sh "+filePathHdfs+" "+fileName+"2250-hadoop");
					copyRes = LinuxShell.runShell("cd "+ConfigData.workdingDirHdfs+" && "+"sh hadoop-copy.sh "+filePathHdfs+" "+fileName+"2250-hadoop").replaceAll("\r|\n", "");
					if(copyRes.equals("completed")){
						logger.info("copy file:"+fileName+"after compare from 2250--hdfs succeeded!!!");
					} else {
						logger.error("error!!! file:"+fileName+"cp from 2250---hdfs after comparison failed!!!");
					}
					break;
				case 2:
					// hdfs > 2250, hdfs ----> 2250
					logger.info(fileName+":archiveTime2250 < archiveTimeHdfs");
					// backup files in 2250
					logger.info("command98"+"cd "+ConfigData.workdingDirHdfs+" && "+"sh hadoop-copy.sh "+filePathHdfs+" "+fileName+"hadoop-2250");
					copyRes = LinuxShell.runShell("cd "+ConfigData.workdingDirHdfs+" && "+"sh hadoop-copy.sh "+filePathHdfs+" "+fileName+"hadoop-2250").replaceAll("\r|\n", "");
					if(copyRes.equals("completed")){
						logger.info("copy file:"+fileName+"from hdfs---2250 after comparason, succeed!!!");
					} else {
						logger.info("copy file:"+fileName+"from hdfs---2250 after coomparison failed!!!");
					}
					break;
				case 3:
					// TODO: SAME ARCHIVE TIME, DO NOTHING
					logger.info(fileName+":archiveTime2250 == archiveTimeHdfs");
					break;
				}
			}
		} else if (!boo2250 && booHdfs) {
			// hdfs ----> 2250
			logger.info(fileName+":!boo2250 && booHdfs");
			logger.info("command112"+"cd "+ConfigData.workdingDirHdfs+" && "+"sh hadoop-copy.sh "+filePathHdfs+" "+fileName+"hadoop-2250");
			copyRes = LinuxShell.runShell("cd "+ConfigData.workdingDirHdfs+" && "+"sh hadoop-copy.sh "+filePathHdfs+" "+fileName+"hadoop-2250").replaceAll("\r|\n", "");
			if(copyRes.equals("completed")){
				logger.info("copy file:"+fileName+"from hdfs----2250, succeeded!!!");
			} else {
				logger.error("copy file:"+fileName+"from hdfs----2250, failed!!!");
			}
		} else if (boo2250 && !booHdfs) {
			// 2250---> hdfs
			logger.info(fileName+":boo2250 && !booHdfs");
			logger.info("command 119"+"cd "+ConfigData.workdingDirHdfs+" && "+"sh hadoop-copy.sh "+filePathHdfs+" "+fileName+"2250-hadoop");
			copyRes = LinuxShell.runShell("cd "+ConfigData.workdingDirHdfs+" && "+"sh hadoop-copy.sh "+filePathHdfs+" "+fileName+"2250-hadoop").replaceAll("\r|\n", "");
			if(copyRes.equals("completed")){
				logger.info("copy file:"+fileName+" from 2250---->hdfs succeeded!!!");
			} else {
				logger.error("error !!!copy file:"+fileName+" from 2250---->hdfs succeeded!!!");
				
			}
		} else {
			logger.info("both files are smaller than 2kb"+fileName);
		}
	}

	/**
	 * fileName exit in target container for rule: fileName-archieveTime
	 * file-fileSize flie-md5sum
	 * */
	public int validateFile(String fileName, Iterator<String> iter225x) {
		String fileNameToValidate;
		// System.out.println("validating...file:" + fileName);
		int validationcount = 0;
		while (iter225x.hasNext()) {
			fileNameToValidate = (String) iter225x.next();
			if ((fileName + "---archiveTime").equals(fileNameToValidate)) {
				validationcount++;
			}
			if ((fileName + "---fileSize").equals(fileNameToValidate)) {
				validationcount++;
			}
			if ((fileName + "---md5sum").equals(fileNameToValidate)) {
				validationcount++;
			}
			if (validationcount == 3) {
				return 1;
			}
		}
		return 0;
	}

	public int checkFileExistAfterValidation(Set<String> keyset, String fileName) {
		// get iterator again
		Iterator<String> iter = keyset.iterator();
		String fileNameSub;
		while (iter.hasNext()) {
			fileNameSub = iter.next().split("---")[0];
			if (fileName.equals(fileNameSub)) {
				return 1;
			}
		}
		return -1;
	}

	// return the position of the param
	public int compare2Time(String server1, String server2) {

		String latestTime = Converter.compareStringASDate(server1, server2);
		if (latestTime == server1) {
			return 1;
		} else if (latestTime == server2) {
			return 2;
		} else if (latestTime.equals("=")) {
			return 3;
		}
		return 0;
	}
}
