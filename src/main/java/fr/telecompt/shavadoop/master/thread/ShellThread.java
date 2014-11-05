package fr.telecompt.shavadoop.master.thread;

import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.util.PropertiesReader;

public class ShellThread extends Thread {

	protected int shellPort;
	protected String hostname;
	protected String fileToTreat;
	protected String dsaKey;
	protected Shell shell;
	protected String hostMapper;
	protected int nbWorker;
	
	public ShellThread(String _dsaKey, String _hostname, String _fileToTreat, String _hostMapper) {
		hostname = _hostname;
		fileToTreat = _fileToTreat;
		dsaKey = _dsaKey;
		hostMapper = _hostMapper;

		PropertiesReader prop = new PropertiesReader();
		shellPort = Integer.parseInt(prop.getPropValues(PropertiesReader.PORT_SHELL));
		nbWorker = Integer.parseInt(prop.getPropValues(PropertiesReader.NB_WORKER));

	}
	
}