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
	private boolean fromLocal;
	private boolean jar;
	
	
	public FileTransfert(SSHManager _sm, String _HostOwner, String _fileToTreat, String _destFile, boolean _fromLocal, boolean _jar) {
		super(_sm, _HostOwner, _fileToTreat);
		destFile = _destFile;
		fromLocal = _fromLocal;
		jar = _jar;
	}
	
	
	public void run() {
		transferFileScp();
	}
	
	
	/**
	 * Transfer a file with scp
	 */
	public void transferFileScp() {
//		cat file | ssh ajw@dogmatix "cat > remote"
//		Or:
//
//		ssh ajw@dogmatix "cat > remote" < file
//		To receive a file:
//
//		ssh ajw@dogmatix "cat remote" > copy
		String cmd = "cat "; //TODO don't work
		String esp = " > ";
		if (jar) {
			cmd = "scp ";
			esp = " ";
		}
		//if to local
		if (sm.isLocal(distantHost)) {
			cmd = cmd + fileToTreat + " " + destFile;
		} else {
			if (fromLocal) {
				// local to distant
				cmd = cmd + fileToTreat + esp + username + "@" + distantHost + ":" + destFile;
			} else {
				// distant to local
				cmd = cmd + username + "@" + distantHost + ":" + fileToTreat + esp + destFile;
			}
		}
		try {
			// Run a java app in a separate system process
			Process p = Runtime.getRuntime().exec(cmd);
	        BufferedReader stdOut=new BufferedReader(new InputStreamReader(p.getInputStream()));
	        String s;
	        while((s=stdOut.readLine())!=null){
	            //nothing
	        }
            p.waitFor();
			if (Constant.MODE_DEBUG) System.out.println("On local : " + cmd);
            p.destroy();
		} catch (Exception e) {
			System.out.println("Error on local : " + cmd);
			e.printStackTrace();
		}
	}
	
}
