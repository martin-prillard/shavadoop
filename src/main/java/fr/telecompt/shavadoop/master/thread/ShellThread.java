package fr.telecompt.shavadoop.master.thread;

import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.master.SSHManager;
import fr.telecompt.shavadoop.util.PropReader;

public class ShellThread extends Thread {

	protected int shellPort;
	protected String distantHost;
	protected String fileToTreat;
	protected String dsaKey;
	protected Shell shell;
	protected int nbWorker;
	protected String username;
	protected SSHManager sm;
	protected boolean local = false;
	
	public ShellThread(SSHManager _sm, String _distantHost, String _fileToTreat) {
		sm = _sm;
		distantHost = _distantHost;
		fileToTreat = _fileToTreat;
		
		username = sm.getUsername();
		shellPort = sm.getShellPort();
		dsaKey = sm.getDsaKey();
		
		local = sm.isLocal(distantHost);
				
		PropReader prop = new PropReader();
		nbWorker = Integer.parseInt(prop.getPropValues(PropReader.WORKER_MAX));
	}
	
}