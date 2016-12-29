package jgrunert.osm_routing_app;

import java.io.File;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


/**
 * Utility to convert osm.pbf file into routing graph for JMapNavigator
 *
 * @author Jonas Grunert
 *
 */
public class OsmAppPreprocessor {

	public static final Logger LOG = Logger.getLogger("OsmAppPreprocessor");

	static {
		FileHandler fh;
		try {
			fh = new FileHandler("passes.log");
			fh.setFormatter(new SimpleFormatter());
			LOG.addHandler(fh);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {
			if (args.length < 2) {
				LOG.info("Invalid number of arguments");
				printHelp();
				return;
			}

			// Input parameters
			String inFilePath = args[0];
			File inFile = new File(inFilePath);

			String outDirPath = args[1];
			File outDir = new File(outDirPath);

			boolean reduceL2Nodes = true;
			for (int i = 2; i < args.length; i++) {
				if (args[i].equals("-l2")) reduceL2Nodes = false;
			}


			// Check parameters
			if (!inFile.exists() || !inFile.isFile()) {
				LOG.severe("Invalid input file given: " + inFilePath);
				printHelp();
				return;
			}

			if (!outDir.exists() || !outDir.isDirectory()) {
				LOG.severe("Invalid output directory given: " + outDirPath);
				printHelp();
				return;
			}

			LOG.info("In: " + inFilePath);
			LOG.info("Out: " + outDirPath);
			LOG.info("reduceL2Nodes: " + reduceL2Nodes);


			// Start preprocessing
			LOG.info("Starting processing");

			new Osm2Graph().doProcessing(inFilePath, outDirPath, reduceL2Nodes);

			LOG.info("Finished processing");
		}
		catch (Exception e) {
			LOG.severe("Failure at main");
			LOG.log(Level.SEVERE, "Exception", e);
		}
	}


	private static void printHelp() {
		System.out.println("Utility to convert osm.pbf to files for JMapNavigator");
		System.out.println("Usage: [InputFile] [OutputDirectory] [optional -l2]");
		System.out.println("   [InputFile]: osm.pbf input file");
		System.out.println("   [OutputDirectory]: Folder directory to write output to");
		System.out.println("   -l2: Disables level2-node-removal which reduces graph to road network by removing non crossing nodes");
	}
}
