package fr.telecompt.shavadoop.master;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import fr.telecompt.shavadoop.master.thread.MapThread;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Nfs;

/**
 * "/cal/homes/prillard/.ssh/id_dsa"
 *
 */
public class Master
{
	
	List<String> filesToMap = new ArrayList<String>();
	Map<String, String> hostMappers = new HashMap<String, String>();
	
	public Master(String dsaFile, String fileIpAdress, String fileWordCount) {
        System.out.println("Shavadoop word-count workflow on : " + fileWordCount);
        //Step 1 : split the file
        inputSplitting(fileWordCount);
        manageMapThread(dsaFile, fileIpAdress);
        //
        shufflingMaps();
	}
    
	public String getDsaKey(String dsaFile) {
		String dsaKey="";

		try {
			InputStream ips=new FileInputStream(dsaFile); 
			InputStreamReader ipsr=new InputStreamReader(ips);
			BufferedReader br=new BufferedReader(ipsr);
			String line;
			while((line=br.readLine())!=null){
				dsaKey+=line+"\n";
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return dsaKey;
	}
    /**
     * Split the original file
     * @param originalFile
     */
    public void inputSplitting(String originalFile) {
		 try {
             FileReader fic = new FileReader(originalFile);
             BufferedReader read = new BufferedReader(fic);
             String line = null;
             int nbFile = 1;
             
             while ((line = read.readLine()) != null) {
            	 //For each line, we write a new file
            	 String fileToMap = "S" + nbFile;
            	 Nfs.postFileToNFS(fileToMap, line);
            	 //We save names of theses files in a list
            	 filesToMap.add(fileToMap);
            	 ++nbFile;
             }
             
             fic.close();
             read.close();   
         } catch (IOException e) {
             e.printStackTrace();
         }
		 System.out.println("Input splitting step done");
    }

    /**
     * Launch a thread to execute map on each distant computer
     * @param fileIpAdress
     */
    public void manageMapThread(String dsaFile, String fileIpAdress) {
    	//Get dsa key
    	String dsaKey = getDsaKey(dsaFile);
    	//Get our hostname mappers
		List<String> hostnameMappers = getHostMappers(fileIpAdress, filesToMap.size());
		//Object to synchronize threads
		ExecutorService es = Executors.newCachedThreadPool();
		//For each files to map
    	for (int i = 0; i < filesToMap.size(); i++) {
    		//We launch a map Thread to execute the map process on the distant computer
			es.execute(new MapThread(dsaKey, hostnameMappers.get(i), filesToMap.get(i)));
       	 	//We save the name of the file and the mapper
        	hostMappers.put(filesToMap.get(i), hostnameMappers.get(i));
    	}
		es.shutdown();
		//Wait while all the threads are not finished yet
		try {
			es.awaitTermination(1, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Parallel maps step done");

    }
    
    /**
     * Return a hostname to do a map
     * @param fileIpAdress
     * @param nbMax
     * @return
     */
    public List<String> getHostMappers(String fileIpAdress, int nbMax) {
    	List<String> hostnameMappers = new ArrayList<String>();

		 try {
             FileReader fic = new FileReader(fileIpAdress);
             BufferedReader read = new BufferedReader(fic);
             String line = null;
             int nbHost = 0;
             
             while ((line = read.readLine()) != null
            		 && nbHost < nbMax) {
            	 hostnameMappers.add(line);
            	 ++nbHost;
             }
             fic.close();
             read.close();   
             
             //TODO optimize and combine function in slave project
    		 if (nbHost != nbMax) {
    			 System.out.println("Not enought host computers to execute the WordCount job");
    			 System.exit(1);
    		 }
    		 
         } catch (IOException e) {
             e.printStackTrace();
         }
		 
    	return hostnameMappers;
    }
    
    public void shufflingMaps() {
    	
    }
}
