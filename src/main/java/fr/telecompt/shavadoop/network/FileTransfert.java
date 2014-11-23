package fr.telecompt.shavadoop.network;

import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.util.Constant;

/**
 * 
 * @author martin prillard
 *
 */
public class FileTransfert extends ShellThread {
	
	private String destFile;
	private boolean jar;
	private Shell shell;
	
	
	public FileTransfert(SSHManager _sm, Shell _shell, String _HostOwner, String _fileToTreat, String _destFile, boolean _jar) {
		super(_sm, _HostOwner, _fileToTreat);
		destFile = _destFile;
		jar = _jar;
		shell = _shell;
	}
	
	
	public void run() {
		transferFileScp();
	}
	
	
	/**
	 * Transfer a file with scp
	 */
	public void transferFileScp() {
		
		String cmd = "cat "; //TODO work, but it's not a good solution
		String esp = " > ";
		if (jar) {
			cmd = "scp "; //TODO work, but sometimes not (when a lot scp launched simultaneous)
			esp = " ";
		}
		//if from local to local
		if (sm.isLocal(distantHost)) {
			cmd = cmd + fileToTreat + " " + destFile;
		//if from local to distant
		} else {
			cmd = cmd + fileToTreat + esp + username + "@" + distantHost + ":" + destFile;
		}
		try {
			if (shell == null) {
				Process p = Runtime.getRuntime().exec(cmd);
//		        BufferedReader stdOut=new BufferedReader(new InputStreamReader(p.getInputStream()));
//		        String s;
//		        while((s=stdOut.readLine())!=null){ //TODO needed ?
//		            //nothing
//		        }
	            p.waitFor();
	            p.destroy();
			} else {
				new Shell.Plain(shell).exec(cmd); //TODO works, but not a good solution
			}
			if (Constant.MODE_DEBUG) System.out.println("On local : " + cmd);

		} catch (Exception e) {
			System.out.println("Error on local : " + cmd);
			e.printStackTrace();
		}
	}
	
}
