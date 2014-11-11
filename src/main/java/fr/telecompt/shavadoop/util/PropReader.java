package fr.telecompt.shavadoop.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropReader {

	private final String URL_CONFIG_FILE = "config.properties";
	public static final String FILE_DSA = "file_dsa";
	public static final String FILE_IP_ADRESS = "file_ip_adress";
	public static final String FILE_INPUT = "file_input";
	public static final String PORT_MASTER = "port_master";
	public static final String PORT_SHELL = "port_shell";
	public static final String WORKER_MAX = "worker_max";
	public static final String THREAD_MAX_BY_WORKER = "thread_max_by_worker";
	public static final String THREAD_LIFETIME = "thread_lifetime";
	public static final String PATH_MASTER = "path_master";
	public static final String PATH_SLAVE = "path_slave";
	public static final String NETWORK_IP_REGEX = "network_ip_regex";
	public static final String MODE_DEBUG = "mode_debug";
	public static final String MODE_SCP_FILES = "mode_scp_files";
	
	public String getPropValues(String key) {
		 
		Properties prop = new Properties();
		
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(URL_CONFIG_FILE);
		try {
			prop.load(inputStream);
		} catch (IOException e) {e.printStackTrace();}

		return prop.getProperty(key);
	}
	
	public void setPropValue(String key, String value) throws IOException {
		Properties prop = new Properties();
		
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(URL_CONFIG_FILE);
		prop.load(inputStream);
		if (inputStream == null) {
			throw new FileNotFoundException("property file '" + URL_CONFIG_FILE + "' not found in the classpath");
		}

		prop.setProperty(key, value);
	}
	
}
