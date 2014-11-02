package fr.telecompt.shavadoop.master.thread;

import com.jcabi.ssh.Shell;

public class ShellThread extends Thread {

	protected final int HOSTNAME_PORT = 22;
	protected final String USERNAME_MASTER = "prillard";
	protected String hostname;
	protected String fileToTreat;
	protected String dsaKey;
	protected Shell shell;
	
	public ShellThread(String _dsaKey, String _hostname, String _fileToTreat) {
		hostname = _hostname;
		fileToTreat = _fileToTreat;
		dsaKey = _dsaKey;
	}
	
}
