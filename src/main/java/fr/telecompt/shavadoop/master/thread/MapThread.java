package fr.telecompt.shavadoop.master.thread;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

public class MapThread extends Thread {

	private String hostname;
	private String fileToMap;
	private String dsaKey;
	
	public MapThread(String _dsaKey, String _hostname, String _fileToMap) {
		hostname = _hostname;
		fileToMap = _fileToMap;
		dsaKey = _dsaKey;
	}
	
	public void run() {
		try {
			//Connect to the distant computer
			Shell shell = new SSH(hostname, 22, "prillard", dsaKey); //TODO change prillard

			//Launch map process
			String pathJar = "slave.jar"; //TODO change pathjar
			new Shell.Plain(shell).exec("java -jar " + pathJar + " map " + fileToMap);
		} catch (Exception e) {
			System.out.println("Fail to connect to " + hostname);
		}
	}
}
