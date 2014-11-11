package fr.telecompt.shavadoop.master.thread;

import org.apache.commons.io.FilenameUtils;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.master.SSHManager;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;

public class LaunchSplitMapping extends ShellThread {
	
	private String hostMapper;
	private boolean local;
	
	public LaunchSplitMapping(SSHManager _sm, String _distantHost, String _fileToTreat, boolean _local, String _hostMapper) {
		super(_sm, _distantHost, _fileToTreat);
		hostMapper = _hostMapper;
		local = _local;
	}
	
	public void run() {
		
		String pathJar = Constant.PATH_JAR;
		String method = Slave.SPLIT_MAPPING_FUNCTION;

		// execute on the master's computer
		if(local) {
			try {
				// Run a java app in a separate system process
				String cmd = getCmdJar(pathJar, hostMapper, method, fileToTreat);
				Process p = Runtime.getRuntime().exec(cmd);
				if (Constant.MODE_DEBUG) System.out.println("On local : " + cmd);
				p.waitFor();
			} catch (Exception e1) {
				System.out.println("Fail to launch shavadoop slave from " + distantHost);
			}
		// execute on a distant computer
		} else {
			try {
				// connect to the distant computer
				shell = new SSH(distantHost, shellPort, Constant.USERNAME, dsaKey);
				
				if (Constant.MODE_SCP_FILES) {
					// MASTER files -> SLAVE
					String destFile = Constant.PATH_SLAVE + FilenameUtils.getBaseName(fileToTreat);
					FileTransfert ft = new FileTransfert(sm, distantHost, fileToTreat, destFile);
					ft.transfertFileScp();
					fileToTreat = destFile;
				}
				
				String cmd = getCmdJar(pathJar, hostMapper, method, fileToTreat);
				
				// launch map process
				new Shell.Plain(shell).exec(cmd);
				if (Constant.MODE_DEBUG) System.out.println("On " + distantHost + " : " + cmd);
				
			} catch (Exception e) {
				System.out.println("Fail to connect to " + hostMapper);
			} 
		}
	}

}