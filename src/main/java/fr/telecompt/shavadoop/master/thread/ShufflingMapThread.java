package fr.telecompt.shavadoop.master.thread;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;

public class ShufflingMapThread extends ShellThread {

	private String key;
	
	public ShufflingMapThread(String _dsaKey, String _hostname, String _filesToShufflingMap, String _key) {
		super(_dsaKey, _hostname, _filesToShufflingMap);
		key = _key;
	}
	
	public void run() {
		try {
			//Connect to the distant computer
			shell = new SSH(hostname, shellPort, usernameMaster, dsaKey);
			//Launch map process
			String pathJar = Constant.APP_FULL_PATH;
			String method = Slave.SHUFFLING_MAP_FUNCTION;
			new Shell.Plain(shell).exec("java -jar " 
				+ pathJar 
				+ " " 
				+ method 
				+ " " 
				+ fileToTreat
				+ " "
				+ key);
		} catch (Exception e) {
			System.out.println("Fail to connect to " + hostname);
		}
	}
	
}
