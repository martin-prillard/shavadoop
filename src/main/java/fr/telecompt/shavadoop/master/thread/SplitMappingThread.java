package fr.telecompt.shavadoop.master.thread;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;

public class SplitMappingThread extends ShellThread {
	
	public SplitMappingThread(String _dsaKey, String _hostname, String _fileToTreat) {
		super(_dsaKey, _hostname, _fileToTreat);
	}
	
	public void run() {
		try {
			//Connect to the distant computer
			shell = new SSH(hostname, shellPort, usernameMaster, dsaKey);
			//Launch map process
			String pathJar = Constant.APP_FULL_PATH;
			String method = Slave.SPLIT_MAPPING_FUNCTION;
			String stdout = new Shell.Plain(shell).exec("java -jar " 
			+ pathJar 
			+ " " 
			+ method 
			+ " " 
			+ fileToTreat
			+ " "
			+ null);
			
		} catch (Exception e) {
			System.out.println("Fail to connect to " + hostname);
		}
	}
}
