package fr.telecompt.shavadoop.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesReader {

	private final String URL_CONFIG_FILE = "config.properties";
	public static final String FILE_DSA = "file_dsa";
	public static final String FILE_IP_ADRESS = "file_ip_adress";
	public static final String FILE_INPUT = "file_input";
	public static final String PORT_MASTER = "port_master";
	public static final String PORT_SHELL = "port_shell";
	public static final String NB_WORKER = "nb_worker";
	
	public String getPropValues(String key) throws IOException {
		 
		Properties prop = new Properties();
		
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(URL_CONFIG_FILE);
		prop.load(inputStream);
		if (inputStream == null) {
			throw new FileNotFoundException("property file '" + URL_CONFIG_FILE + "' not found in the classpath");
		}

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
