package fr.telecompt.shavadoop.master.thread;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.master.SSHManager;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;

public class LaunchShufflingMap extends ShellThread {

	public LaunchShufflingMap(SSHManager _sm, String _distantHost, String _shufflingDictionaryFile) {
		super(_sm, _distantHost, _shufflingDictionaryFile);
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
				Process p = Runtime.getRuntime().exec(cmd);
				if (Constant.MODE_DEBUG) System.out.println("On local : " + cmd);
				p.waitFor();
			} catch (Exception e1) {
				System.out.println("Fail to launch shavadoop slave from " + distantHost);
			}
		// execute on a distant computer
		} else {
			try {
				//Connect to the distant computer
				shell = new SSH(distantHost, shellPort, Constant.USERNAME_MASTER, dsaKey);
				 
				//Launch map process
				new Shell.Plain(shell).exec(cmd);
				if (Constant.MODE_DEBUG) System.out.println("On " + distantHost + " : " + cmd);
				
			} catch (Exception e) {
				System.out.println("Fail to connect to " + distantHost);
			} 
		}
	}
	
}