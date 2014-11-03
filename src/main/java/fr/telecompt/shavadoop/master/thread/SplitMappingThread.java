package fr.telecompt.shavadoop.master.thread;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.slave.Slave;

public class SplitMappingThread extends ShellThread {
	
	public SplitMappingThread(String _dsaKey, String _hostname, String _fileToMap) {
		super(_dsaKey, _hostname, _fileToMap);
	}
	
	public void run() {
		try {
			//Connect to the distant computer
			shell = new SSH(hostname, shellPort, usernameMaster, dsaKey);
			//Launch map process
			String pathJar = "shavadoop.jar"; //TODO change pathjar
			String method = Slave.SPLIT_MAPPING_FUNCTION;
			new Shell.Plain(shell).exec("java -jar " 
			+ pathJar 
			+ " " 
			+ method 
			+ " " 
			+ fileToTreat);
		} catch (Exception e) {
			System.out.println("Fail to connect to " + hostname);
		}
	}
}
