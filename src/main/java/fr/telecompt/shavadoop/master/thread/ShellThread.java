package fr.telecompt.shavadoop.master.thread;

import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.util.PropReader;

public class ShellThread extends Thread {

	protected boolean local;
	protected int shellPort;
	protected String hostname;
	protected String fileToTreat;
	protected String dsaKey;
	protected Shell shell;
	protected String hostMapper;
	protected int nbWorker;
	
	public ShellThread(Boolean _local, String _dsaKey, String _hostname, String _fileToTreat, String _hostMapper) {
		local = _local;
		hostname = _hostname;
		fileToTreat = _fileToTreat;
		dsaKey = _dsaKey;
		hostMapper = _hostMapper;

		PropReader prop = new PropReader();
		shellPort = Integer.parseInt(prop.getPropValues(PropReader.PORT_SHELL));
		nbWorker = Integer.parseInt(prop.getPropValues(PropReader.WORKER_MAX));

	}
	
}