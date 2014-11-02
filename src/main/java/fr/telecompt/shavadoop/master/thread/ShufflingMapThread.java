package fr.telecompt.shavadoop.master.thread;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

public class ShufflingMapThread extends ShellThread {

	public ShufflingMapThread(String _dsaKey, String _hostname, String _filesToShufflingMap) {
		super(_dsaKey, _hostname, _filesToShufflingMap);
	}
	
	public void run() {
		try {
			//Connect to the distant computer
			shell = new SSH(hostname, HOSTNAME_PORT, USERNAME_MASTER, dsaKey);
			//Launch map process
			String pathJar = "shavadoop.jar"; //TODO change pathjar
			String method = "shuffling_map_function";
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
