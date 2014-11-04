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
	protected String hostMapper;
	protected int nbWorker;
	
	public ShellThread(String _usernameMaster, String _dsaKey, String _hostname, String _fileToTreat, String _hostMapper) {
		usernameMaster = _usernameMaster;
		hostname = _hostname;
		fileToTreat = _fileToTreat;
		dsaKey = _dsaKey;
		hostMapper = _hostMapper;

		PropertiesReader prop = new PropertiesReader();
		try {
			shellPort = Integer.parseInt(prop.getPropValues(PropertiesReader.PORT_SHELL));
			nbWorker = Integer.parseInt(prop.getPropValues(PropertiesReader.NB_WORKER));
		} catch (IOException e) {e.printStackTrace();}

	}
	
}
