package fr.telecompt.shavadoop.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

public class Util {

	/**
	 * Create file from map
	 * @param nameFile
	 * @param content
	 */
	public static void writeFile(String nameFile, Map<String, Integer> content) {
		try {
			FileWriter fw = new FileWriter (nameFile);
			BufferedWriter bw = new BufferedWriter (fw);
			PrintWriter write = new PrintWriter (bw); 
			
			for(Entry<String, Integer> entry : content.entrySet()) {
			    write.println (entry.getKey()
			    		+ Constant.SEP_CONTAINS_FILE
			    		+ entry.getValue()); 
			}
			
			write.close();
			bw.close();
			fw.close();
		}
		catch (Exception e){e.printStackTrace();}
	}
	
	public static void writeFile(String nameFile, List<String> content) {
		try {
			FileWriter fw = new FileWriter (nameFile);
			BufferedWriter bw = new BufferedWriter (fw);
			PrintWriter write = new PrintWriter (bw); 
			
			for(String line : content) {
			    write.println (line); 
			}
			
			write.close();
			bw.close();
			fw.close();
		}
		catch (Exception e){e.printStackTrace();}
	}
	
	public static void writeFileFromPair(String nameFile, List<Pair> content) {
      	 // If the file exist, we concat
      	 if(new File(nameFile).exists()) {
      		try(
      			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(nameFile, true)))) {
				for(Pair p : content) {
					out.println (p.getVal1()
				    		+ Constant.SEP_CONTAINS_FILE
				    		+ p.getVal2()); 
				}
      		}catch (Exception e) {e.printStackTrace();}
      	 // we create the file
      	 } else {
			try {
				FileWriter fw = new FileWriter (nameFile);
				BufferedWriter bw = new BufferedWriter (fw);
				PrintWriter write = new PrintWriter (bw); 
				
				for(Pair p : content) {
				    write.println (p.getVal1()
				    		+ Constant.SEP_CONTAINS_FILE
				    		+ p.getVal2()); 
				}
				
				write.close();
				bw.close();
				fw.close();
			}
			catch (Exception e){
				System.out.println(e.toString());
			}
      	 }
	}
	
	/**
	 * Create a directory
	 * @param file
	 */
	public static void createDirectory(File file) {
	    // if the directory does not exist, create it
	    if (!file.exists()) {
	     try{
	    	 file.mkdir();
	       } catch(Exception e){e.printStackTrace();}  
	    }
	}
	
	/**
	 * Clean the directory
	 * @param file
	 */
	public static void cleanDirectory(File file) {
		try {
			FileUtils.cleanDirectory(file);
		} catch (IOException e) {e.printStackTrace();}
	}
	
	public static void initializeResDirectory(String pathRepoRes) {
		Pattern paternRootPath = Pattern.compile(Constant.PATH_ROOT);
		Matcher matcherRootPath = paternRootPath.matcher(pathRepoRes);
		// clean directory
		if (!matcherRootPath.find()) {
			createDirectory(new File(pathRepoRes));
			cleanDirectory(new File(pathRepoRes)); 
			if (Constant.MODE_DEBUG) System.out.println(pathRepoRes + " directory cleaned");
		} else {
			if (Constant.MODE_DEBUG) System.out.println(pathRepoRes + " is the root path ! ");
		}
	}
	
	public static String pairToString(Set<Pair> input) {
		String res = "";
		for (Pair p : input) {
			res += p.getVal1() + Constant.SEP_FILES_SHUFFLING_MAP;
			res += p.getVal2() + Constant.SEP_FILES_SHUFFLING_MAP_GROUP;
		}
		
		// Remove the last separator
	    if (res.length() > 0 && Character.toString(res.charAt(res.length()-1)).equals(Constant.SEP_FILES_SHUFFLING_MAP_GROUP)) {
	    	res = res.substring(0, res.length()-1);
	    }
	    
		return res;
	}
	
    /**
     * Clean the line
     * @param line
     * @return line clean
     */
    private static String cleanLine(String line) {
    	String clean = line;
    	clean = clean.trim();
    	// clean the non alpha numeric character or space
    	clean = clean.replaceAll("[^a-zA-Z0-9\\s]", "");
    	// just one space beetween each words
    	clean = clean.replaceAll("\\s+", " ");
    	clean = clean.replaceAll("\\t+", " ");
    	return clean;
    }
    
	public static ExecutorService fixedThreadPoolWithQueueSize(int nThreads, int queueSize) {
	    return new ThreadPoolExecutor(nThreads, nThreads,
	                                  5000L, TimeUnit.MILLISECONDS,
	                                  new ArrayBlockingQueue<Runnable>(queueSize, true), 
	                                  new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	
	/**
	 * Clean a text
	 * @param fileInput
	 * @param fileOutput
	 */
	public static void cleanText(String fileInput, String fileOutput) {
		FileReader fr;
		try {
			fr = new FileReader(fileInput);
			BufferedReader br = new BufferedReader(fr); 
			FileWriter fw = new FileWriter(fileOutput); 
			BufferedWriter bw = new BufferedWriter (fw);
			PrintWriter write = new PrintWriter(bw); 
			String line;
	
			while((line = br.readLine()) != null)
			{ 
			    line = cleanLine(line);
			    // don't write out blank lines
			    if (!line.equals("") || !line.isEmpty()) 
			    {
			        write.println(line);
			    }
			} 
			write.close();
			br.close();
			fr.close();
		} catch (Exception e) {e.printStackTrace();} 
		
	}
	
	/**
	 * get the number of line of the file
	 * @param file
	 * @return
	 */
	public static int getFileNumberLine(String file) {
		int nbLine = 0;
        FileReader fic;
		try {
			fic = new FileReader(new File(file));
	        LineNumberReader  lnr = new LineNumberReader(fic);
	        lnr.skip(Long.MAX_VALUE);
	        nbLine = lnr.getLineNumber();
	        lnr.close();
		} catch (Exception e) {e.printStackTrace();}

        return nbLine;
	}
	
    /**
     * Split a file by line
     * @param file
     * @param nbLineByHost
     * @param restLineByHost
     * @param filesToMap
     */
    public static List<String> splitByLineFile(String file, int nbLineByHost, int restLineByHost, int nbWorkerMappers) {
    	List<String> filesToMap = new ArrayList<String>();
    	
        try {
            String line = null;
            int nbFile = 0;
            
        	// Content of the file
            List<String> content = new ArrayList<String>();
            FileReader fic = new FileReader(new File(file));
            BufferedReader read = new BufferedReader(fic);
            
			while ((line = read.readLine()) != null) {
			 // Add line by line to the content file
			 content.add(line);
			 // Write the complete file by block or if it's the end of the file
			 if ((content.size() == nbLineByHost && nbFile < nbWorkerMappers - 1)
					 || (content.size() == nbLineByHost + restLineByHost && nbFile == nbWorkerMappers - 1)) {
			   	 //For each group of line, we write a new file
			   	 ++nbFile;
			   	 String fileToMap = Constant.PATH_F_SPLITING + nbFile;
			   	 Util.writeFile(fileToMap, content);
			   	 
			   	 if (Constant.MODE_DEBUG) System.out.println("Input file splited in : " + fileToMap);
			   	 		
			   	 //We save names of theses files in a list
			   	 filesToMap.add(fileToMap);
			   	 // Reset
			   	 content = new ArrayList<String>();
			 }
			}
	        read.close();
	        fic.close();
		} catch (IOException e) {e.printStackTrace();}
        return filesToMap;
    }
    
    /**
     * Split large file by bloc
     * @param file
     */
    public static List<String> splitLargeFile(String file, int nbBlocByHost, int restBlocByHost, int nbWorkerMappers) {
    	List<String> filesToMap = new ArrayList<String>();
    	//TODO couper a la fin des lignes, serialization ?
    	File inputFile = new File(file);
		FileInputStream inputStream;
		FileOutputStream filePart;
		int fileSize = (int) inputFile.length();
		int nbFile = 0;
		int read = 0;
		int readLength = Constant.BLOC_SIZE_MIN;
		
		byte[] byteChunkPart;
		
		try {
			inputStream = new FileInputStream(inputFile);
			
			while (fileSize > 0) {
				if (fileSize <= Constant.BLOC_SIZE_MIN) {
					readLength = fileSize;
				}
				
				byteChunkPart = new byte[readLength];
				read = inputStream.read(byteChunkPart, 0, readLength);
				fileSize -= read;
				assert (read == byteChunkPart.length);
				
				nbFile++;
				String fileToMap = Constant.PATH_F_SPLITING + nbFile;
				filePart = new FileOutputStream(new File(fileToMap));
				filesToMap.add(fileToMap);
				filePart.write(byteChunkPart);
				filePart.flush();
				filePart.close();
				byteChunkPart = null;
				filePart = null;
			}
			
			inputStream.close();
			
		} catch (IOException exception) {
			exception.printStackTrace();
		}
		return filesToMap;
    }
	
}
