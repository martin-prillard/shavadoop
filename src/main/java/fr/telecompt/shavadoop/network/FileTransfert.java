package fr.telecompt.shavadoop.network;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import fr.telecompt.shavadoop.util.Constant;

/**
 * 
 * @author martin prillard
 *
 */
public class FileTransfert extends ShellThread {
	
	private String destFile;
	private boolean fromLocalToDistant;
	private boolean bulk;
	
	public FileTransfert(SSHManager _sm, String _hostOwner, String _fileToTreat, String _destFile, boolean _fromLocalToDistant, boolean _bulk) {
		super(_sm, _hostOwner, _fileToTreat);
		destFile = _destFile;
		fromLocalToDistant = _fromLocalToDistant;
		bulk = _bulk;
	}
	
	
	public void run() {
		transferFileScp();
	}
	
	
	/**
	 * Transfer a file with scp
	 */
	public void transferFileScp() {
		
		if (!sm.isLocal(distantHost)) {
			String cmdLine = null;
			if (fromLocalToDistant) {
				cmdLine = "scp " + fileToTreat + " " + username + "@" + distantHost + ":" + destFile;
			} else {
				if (bulk) {
					cmdLine = "scp " + username + "@" + distantHost + ":{" + fileToTreat + "} " + destFile;
				} else {
					cmdLine = "scp " + username + "@" + distantHost + ":" + fileToTreat + " " + destFile;
				}
			}
			
			try {
				Process p = Runtime.getRuntime().exec(cmdLine);
		        BufferedReader stdOut=new BufferedReader(new InputStreamReader(p.getInputStream()));
		        while((stdOut.readLine())!=null){
		            // do nothing, wait needed for scp
		        }
	            p.waitFor();
	            p.destroy();
				if (Constant.MODE_DEBUG) System.out.println("On local : " + cmdLine);
			} catch (Exception e) {
				System.out.println("Error on local : " + cmdLine);
				e.printStackTrace();
			}
		}
	}
	
}
