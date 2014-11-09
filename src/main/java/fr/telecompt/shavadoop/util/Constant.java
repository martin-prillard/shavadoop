package fr.telecompt.shavadoop.util;

public class Constant {

	public final static String APP_VERSION = "v0.3";
	public final static String APP_EXTENSION = ".jar";
	public static String APP_PATH_REPO = new PropReader().getPropValues(PropReader.APP_PATH_REPO);
	public final static String APP_PATH_REPO_RES = APP_PATH_REPO + "/temp/";
	public final static String APP_PATH_JAR = APP_PATH_REPO + "/shavadoop" + "_" + APP_VERSION + APP_EXTENSION;
	public final static boolean APP_DEBUG = true;
	public final static String APP_DEBUG_TITLE = "---------------------------------------------------------";
	
	public final static String SEPARATOR = " ";
	public final static String FILE_SEPARATOR = ", ";
	public final static String FILES_SHUFFLING_MAP_SEPARATOR = ",";
	public final static String SOCKET_SEPARATOR_MESSAGE = ";";
	public final static String SOCKET_END_MESSAGE = "END";
	
	public final static String F_SPLITING = APP_PATH_REPO_RES + "S";
	public final static String F_MAPPING = APP_PATH_REPO_RES + "UM";
	public final static String F_SHUFFLING_DICTIONARY = APP_PATH_REPO_RES + "DSM";
	public final static String F_SHUFFLING = APP_PATH_REPO_RES + "SM";
	public final static String F_REDUCING = APP_PATH_REPO_RES + "RM";
	public final static String F_FINAL_RESULT = APP_PATH_REPO_RES + "output";
	public final static String F_SEPARATOR = "_";

	public static String USERNAME_MASTER = "";
}
