package cross.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.junit.Test;

public class LinuxShell {

	public static String runShell(String command) {
		Process p;

		StringBuffer sb = new StringBuffer();
		String[] cmd = { "/bin/sh", "-c", command };
		try {
			p = Runtime.getRuntime().exec(cmd);
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";
			while ((line = reader.readLine()) != null) {
				sb.append(line + "\n");
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}
}