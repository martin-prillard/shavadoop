package fr.telecompt.shavadoop.network;

import fr.telecompt.shavadoop.util.Constant;

/**
 * 
 * @author martin prillard
 *
 */
public class FileTransfert extends ShellThread {
	
	private String destFile;
	private boolean fromLocal;
	
	
	public FileTransfert(SSHManager _sm, String _HostOwner, String _fileToTreat, String _destFile, boolean _fromLocal) {
		super(_sm, _HostOwner, _fileToTreat);
		destFile = _destFile;
		fromLocal = _fromLocal;
	}
	
	
	public void run() {
		transferFileScp();
	}
	
	
	/**
	 * Transfer a file with scp
	 */
	public void transferFileScp() {
		String cmd = null;
		//if to local
		if (sm.isLocal(distantHost)) {
			cmd = "scp " + fileToTreat + " " + destFile;
		} else {
			if (fromLocal) {
				// local to distant
				cmd = "scp " + fileToTreat + " " + username + "@" + distantHost + ":" + destFile;
			} else {
				// distant to local
				cmd = "scp " + username + "@" + distantHost + ":" + fileToTreat + " " + destFile;
			}
		}
		try {
			// Run a java app in a separate system process
			Process p = Runtime.getRuntime().exec(cmd);
			if (Constant.MODE_DEBUG) System.out.println("On local : " + cmd);
			p.waitFor();
		} catch (Exception e) {
			System.out.println("Error on local : " + cmd);
			e.printStackTrace();
		}
	}
	
}
