package fr.telecompt.shavadoop.master.thread;

import java.io.IOException;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;

public class ShufflingMapThread extends ShellThread {

	private String key;
	
	public ShufflingMapThread(String _dsaKey, String _hostname, String _fileToTreat, String _key, String _hostMapper) {
		super(_dsaKey, _hostname, _fileToTreat, _hostMapper);
		key = _key;
	}
	
	public void run() {
		
		String pathJar = Constant.APP_PATH_JAR;
		String method = Slave.SHUFFLING_MAP_FUNCTION;
		
		String cmd = "java -jar" 
				+ Constant.SEPARATOR
				+ pathJar
				+ Constant.SEPARATOR
				+ null 
				+ Constant.SEPARATOR
				+ method 
				+ Constant.SEPARATOR 
				+ fileToTreat
				+ Constant.SEPARATOR
				+ key;
		
		try {
			//Connect to the distant computer
			shell = new SSH(hostname, shellPort, Constant.USERNAME_MASTER, dsaKey);

			//Launch map process
			new Shell.Plain(shell).exec(cmd);
			if (Constant.APP_DEBUG) System.out.println(cmd);
			
		} catch (Exception e) {
			// the master is the worker
			if (hostname.equalsIgnoreCase(Constant.USERNAME_MASTER)) {
				Process proc;
				try {
					// Run a java app in a separate system process
					proc = Runtime.getRuntime().exec(cmd);
				} catch (IOException e1) {
					System.out.println("Fail to launch shavadoop slave from " + hostname);
				}
			} else {
				System.out.println("Fail to connect to " + hostMapper);
			}
		} 
	}
	
}
