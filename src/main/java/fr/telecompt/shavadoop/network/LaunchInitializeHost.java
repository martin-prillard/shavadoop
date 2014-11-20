package fr.telecompt.shavadoop.network;

import java.util.concurrent.ExecutorService;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.jcabi.ssh.SSH;
import com.jcabi.ssh.Shell;

import fr.telecompt.shavadoop.util.Constant;

/**
 * 
 * @author martin prillard
 *
 */
public class LaunchInitializeHost extends Thread {

	private SSHManager sm;
	private String host;
	private int shellPort;
	private String dsaKey;
	
	
	public LaunchInitializeHost(SSHManager _sm, ExecutorService _es, String _host) {
		sm = _sm;
		host = _host;
		shellPort = sm.getShellPort();
		dsaKey = sm.getDsaKey();
	}


	/**
	 * Create the directories and jar on distant computer needed to run shavadoop
	 */
	public void run() {
		initializeShavadoopWorkspace();
	}
	
	
	/**
	 * Create the directories and jar on distant computer needed to run shavadoop
	 */
	public void initializeShavadoopWorkspace() {
		Pattern paternRootPath = Pattern.compile(Constant.PATH_ROOT);
		Matcher matcherRootPath = paternRootPath.matcher(Constant.PATH_REPO);
		// clean directory
		if (!matcherRootPath.find()) {
			try {
				Shell shell = new SSH(host, shellPort, Constant.USERNAME, dsaKey);
				new Shell.Plain(shell).exec("rm -rf " + Constant.PATH_REPO);
				new Shell.Plain(shell).exec("mkdir " + Constant.PATH_REPO);
				new Shell.Plain(shell).exec("mkdir " + Constant.PATH_REPO_RES);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		// transfert the jar program if needed
		FileTransfert ft = new FileTransfert(sm, host, Constant.PATH_SHAVADOOP_JAR, Constant.PATH_JAR, true);
		ft.start();
	}
	
}
