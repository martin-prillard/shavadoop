package fr.telecompt.shavadoop.util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Map.Entry;

public class LocalRepoFile {

	private final static String SEPARATOR = "\\s+";
	private final static String FILE_SEPARATOR = ",\\s+";
	
	public static void writeFile(String nameFile, Map<String, Integer> content) {
		try {
			FileWriter fw = new FileWriter (nameFile);
			BufferedWriter bw = new BufferedWriter (fw);
			PrintWriter write = new PrintWriter (bw); 
			for(Entry<String, Integer> entry : content.entrySet()) {
			    write.println (entry.getKey()
			    		+ FILE_SEPARATOR
			    		+ entry.getValue()); 
			}
			write.close();
		}
		catch (Exception e){
			System.out.println(e.toString());
		}
	}
	
}
