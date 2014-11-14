package fr.telecompt.shavadoop.thread;

import java.io.InterruptedIOException;

import org.apache.commons.io.FilenameUtils;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.master.SSHManager;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;

public class LaunchSplitMapping extends ShellThread {
	
	private String hostMapper;
	private boolean local;
	private String idWorker;
	
	public LaunchSplitMapping(SSHManager _sm, String _distantHost, String _fileToTreat, boolean _local, String _hostMapper, String _idWorker) {
		super(_sm, _distantHost, _fileToTreat);
		hostMapper = _hostMapper;
		local = _local;
		idWorker = _idWorker;
	}
	
	public void run() {
		
		try {
			
			String pathJar = Constant.PATH_JAR;
			String method = Slave.SPLIT_MAPPING_FUNCTION;
	
			// execute on the master's computer
			if(local) {
				// Run a java app in a separate system process
				String cmd = getCmdJar(pathJar, hostMapper, method, fileToTreat, idWorker);
				Process p = Runtime.getRuntime().exec(cmd);
				if (Constant.MODE_DEBUG) System.out.println("On local : " + cmd);
				p.waitFor();
			// execute on a distant computer
			} else {
				// connect to the distant computer
				shell = new SSH(distantHost, shellPort, Constant.USERNAME, dsaKey);
				
				if (Constant.MODE_SCP_FILES) {
					// MASTER files -> SLAVE
					String destFile = Constant.PATH_SLAVE + FilenameUtils.getBaseName(fileToTreat);
					FileTransfert ft = new FileTransfert(sm, distantHost, fileToTreat, destFile);
					ft.transfertFileScp();
					fileToTreat = destFile;
				}
				
				String cmd = getCmdJar(pathJar, hostMapper, method, fileToTreat, idWorker);
				
				// launch map process
				String stdout = new Shell.Plain(shell).exec(cmd);
				if (Constant.MODE_DEBUG) System.out.println("On " + distantHost + " : " + cmd);
				if (Constant.MODE_DEBUG) System.out.println("Stdout slave : " + stdout); //TODO
			}
	    } catch (InterruptedIOException e) { // if thread was interrupted
	        Thread.currentThread().interrupt();
	        if (Constant.MODE_DEBUG) System.out.println("TASK_TRACKER : worker failed was interrupted");
	    } catch (Exception e) {
	        if (!isInterrupted()) { // if other exceptions
	        	System.out.println("Fail to launch shavadoop slave from " + distantHost);
	        } else { 
	        	if (Constant.MODE_DEBUG) System.out.println("TASK_TRACKER : worker failed was interrupted");
	        }
	    }
	}

}