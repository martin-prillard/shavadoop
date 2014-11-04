package fr.telecompt.shavadoop.main;

import java.io.IOException;

import fr.telecompt.shavadoop.master.Master;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;
import fr.telecompt.shavadoop.util.PropertiesReader;

public class Main {


    public static void main( String[] args )
    {
    
		// initialize the shavadoop repository
		Constant.APP_PATH_REPO = new PropertiesReader().getPropValues(PropertiesReader.APP_PATH_REPO);
		
    	if (args.length == 0) {
    		// launch the master
        	Master m = new Master();
        	m.initialize();
        	m.launchMapReduce();
    	} else if (args.length == 4){
    		// launch the slave
    		new Slave(args[0], args[1], args[2], args[3]);
    	} else {
    		System.out.println("Not enough args");
    	}
    	
    }
    
}
