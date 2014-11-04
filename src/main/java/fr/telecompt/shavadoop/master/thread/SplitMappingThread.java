package fr.telecompt.shavadoop.master.thread;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;

public class SplitMappingThread extends ShellThread {
	
	private String hostMapper;
	
	public SplitMappingThread(String _dsaKey, String _hostname, String _fileToTreat, String _hostMapper) {
		super(_dsaKey, _hostname, _fileToTreat);
		hostMapper = _hostMapper;
	}
	
	public void run() {
		try {
			//Connect to the distant computer
			shell = new SSH(hostname, shellPort, usernameMaster, dsaKey);
			//Launch map process
			String pathJar = Constant.APP_PATH_JAR;
			String method = Slave.SPLIT_MAPPING_FUNCTION;
			if (Constant.APP_DEBUG) System.out.println("Cmd : " 
					+ "java -jar"
					+ Constant.SEPARATOR
					+ pathJar
					+ Constant.SEPARATOR
					+ hostMapper
					+ Constant.SEPARATOR 
					+ method 
					+ Constant.SEPARATOR 
					+ fileToTreat
					+ Constant.SEPARATOR
					+ null);
			
			new Shell.Plain(shell).exec("java -jar" 
					+ Constant.SEPARATOR
					+ pathJar
					+ Constant.SEPARATOR
					+ hostMapper
					+ Constant.SEPARATOR 
					+ method 
					+ Constant.SEPARATOR 
					+ fileToTreat
					+ Constant.SEPARATOR
					+ null);
			
		} catch (Exception e) {
			System.out.println("Fail to connect to " + hostname);
		}
	}
}
