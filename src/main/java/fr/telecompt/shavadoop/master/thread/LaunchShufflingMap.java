package fr.telecompt.shavadoop.master.thread;

import java.io.IOException;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;

public class LaunchShufflingMap extends ShellThread {

	public LaunchShufflingMap(boolean _local, String _dsaKey, String _hostname, String _shufflingDictionaryFile, String _hostMapper) {
		super(_local, _dsaKey, _hostname, _shufflingDictionaryFile, _hostMapper);
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
				+ fileToTreat;
		
		// execute on the master's computer
		if(local) {
			try {
				// Run a java app in a separate system process
				Runtime.getRuntime().exec(cmd);
				if (Constant.APP_DEBUG) System.out.println("On local : " + cmd);
			} catch (IOException e1) {
				System.out.println("Fail to launch shavadoop slave from " + hostname);
			}
		// execute on a distant computer
		} else {
			try {
				//Connect to the distant computer
				shell = new SSH(hostname, shellPort, Constant.USERNAME_MASTER, dsaKey);

				//Launch map process
				new Shell.Plain(shell).exec(cmd);
				if (Constant.APP_DEBUG) System.out.println("On " + hostname + " : " + cmd);
				
			} catch (Exception e) {
				System.out.println("Fail to connect to " + hostMapper);
			} 
		}
	}
	
}