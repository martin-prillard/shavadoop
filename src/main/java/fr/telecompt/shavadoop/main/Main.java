package fr.telecompt.shavadoop.main;

import fr.telecompt.shavadoop.master.Master;
import fr.telecompt.shavadoop.slave.Slave;
import fr.telecompt.shavadoop.util.Constant;

public class Main {


    public static void main( String[] args )
    {

    	if (args.length == 0) {
        	Master m = new Master();
        	m.initialize();
        	m.launchMapReduce();
    	} else if (args.length == 4){
    		new Slave(args[0], args[1], args[2], args[3]);
    	} else {
    		System.out.println("Not enough args");
    	}
    }
    
}
