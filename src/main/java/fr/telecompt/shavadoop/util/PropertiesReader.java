package fr.telecompt.shavadoop.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesReader {

	private final String URL_CONFIG_FILE = "res/config.properties";
	public static final String DSA_FILE = "dsa_file";
	public static final String IP_ADRESS_FILE = "ip_adress_file";
	
	public String getPropValues(String key) throws IOException {
		 
		Properties prop = new Properties();
		
		InputStream inputStream = getClass().getClassLoader().getResourceAsStream(URL_CONFIG_FILE);
		prop.load(inputStream);
		if (inputStream == null) {
			throw new FileNotFoundException("property file '" + URL_CONFIG_FILE + "' not found in the classpath");
		}

		return prop.getProperty(key);
	}
	
}
