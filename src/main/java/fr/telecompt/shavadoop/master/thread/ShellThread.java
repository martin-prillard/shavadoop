package fr.telecompt.shavadoop.master.thread;

import java.io.IOException;

import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.util.PropertiesReader;

public class ShellThread extends Thread {

	protected int shellPort;
	protected String usernameMaster;
	protected String hostname;
	protected String fileToTreat;
	protected String dsaKey;
	protected Shell shell;
	
	public ShellThread(String _dsaKey, String _hostname, String _fileToTreat) {
		hostname = _hostname;
		fileToTreat = _fileToTreat;
		dsaKey = _dsaKey;

		PropertiesReader prop = new PropertiesReader();
		try {
			usernameMaster = prop.getPropValues(PropertiesReader.USERNAME_MASTER);
			shellPort = Integer.parseInt(prop.getPropValues(PropertiesReader.SHELL_PORT));
		} catch (IOException e) {e.printStackTrace();}

	}
	
}
