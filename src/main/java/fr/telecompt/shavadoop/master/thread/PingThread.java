package fr.telecompt.shavadoop.master.thread;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

public class PingThread extends ShellThread {
	
	public PingThread(String _username, String _dsaKey, String _hostname, String _fileToTreat, String _hostnameMaster) {
		super(_username, _dsaKey, _hostname, _fileToTreat, _hostnameMaster);
	}

	public void run() {
		try {
			//Connect to the distant computer
			shell = new SSH(hostname, shellPort, usernameMaster, dsaKey);
			
			//Ping distant computer
			new Shell.Plain(shell).exec("ping " + hostname);
			//TODO ?
//			while(true) {
//				
//			}
		} catch (Exception e) {
			System.out.println("Fail to connect to " + hostname);
		}
	}
}
