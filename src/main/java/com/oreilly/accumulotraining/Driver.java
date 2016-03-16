package com.oreilly.accumulotraining;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Driver {

	private static final Options options;

	static {
		options = new Options();

		Option zo = OptionBuilder.withArgName("z")
				.hasArg()
				.withLongOpt("zookeeper")
				.withDescription("ZooKeeper servers (comma separated)")
				.create("z");
		options.addOption(zo);

		Option io = OptionBuilder.withArgName("i")
				.hasArg()
				.withLongOpt("instance")
				.withDescription("Instance")
				.create("i");
		options.addOption(io);

		Option uo = OptionBuilder.withArgName("u")
				.hasArg()
				.withLongOpt("username")
				.withDescription("Username")
				.create("u");
		options.addOption(uo);

		Option po = OptionBuilder.withArgName("p")
				.hasArg()
				.withLongOpt("password")
				.withDescription("Password")
				.create("p");
		options.addOption(po);

		Option to = OptionBuilder.withArgName("table")
				.hasArg()
				.withLongOpt("table")
				.withDescription("Table")
				.create("t");
		options.addOption(to);
    
    Option oto = OptionBuilder.withArgName("outputTable")
				.hasArg()
				.withLongOpt("outputTabe")
				.withDescription("Output Table")
				.create("ot");
		options.addOption(oto);

		Option fo = OptionBuilder.withArgName("file")
				.hasArg()
				.withLongOpt("file")
				.withDescription("CSV file")
				.create("f");
		options.addOption(fo);

		Option ro = OptionBuilder.withArgName("r")
				.hasArg()
				.withLongOpt("row")
				.withDescription("Row")
				.create("r");
		options.addOption(ro);
		
		Option cfo = OptionBuilder.withArgName("cf")
				.hasArg()
				.withLongOpt("columnFamily")
				.withDescription("Column Family")
				.create("cf");
		options.addOption(cfo);
		
		Option cqo = OptionBuilder.withArgName("cq")
				.hasArg()
				.withLongOpt("columnQualifier")
				.withDescription("Column Qualifier")
				.create("cq");
		options.addOption(cqo);
		
		Option nvo = OptionBuilder.withArgName("nv")
				.hasArg()
				.withLongOpt("newValue")
				.withDescription("New Value")
				.create("nv");
		options.addOption(nvo);
		
		Option svo = OptionBuilder.withArgName("sv")
				.hasArg()
				.withLongOpt("startValue")
				.withDescription("Start Value")
				.create("sv");
		options.addOption(svo);
		
		Option evo = OptionBuilder.withArgName("ev")
				.hasArg()
				.withLongOpt("endValue")
				.withDescription("End Value")
				.create("ev");
		options.addOption(evo);
	}

	public static void main(String[] args) throws Exception {

		// create the parser
		CommandLineParser optionsParser = new BasicParser();

		try {
			// parse the command line arguments
			CommandLine cmdline = optionsParser.parse(options, args);
			
			String instanceName = cmdline.getOptionValue("i");
			String zookeepers = cmdline.getOptionValue("z");
			String username = cmdline.getOptionValue("u");
			String password = cmdline.getOptionValue("p");
			String table = cmdline.getOptionValue("t");
      String outputTable = cmdline.getOptionValue("ot");
			
			String filename = cmdline.getOptionValue("f");
			
			String row = cmdline.getOptionValue("r");
			String columnFamily = cmdline.getOptionValue("cf");
			String columnQualifier = cmdline.getOptionValue("cq");
			
			String newValue = cmdline.getOptionValue("nv");
			String startValue = cmdline.getOptionValue("sv");
			String endValue = cmdline.getOptionValue("ev");
			
      if(cmdline.getArgList().isEmpty()) {
        throw new ParseException("");
      }
      
			String mode = (String) cmdline.getArgList().get(0);

			switch (mode) {
				case "write":
					IngestClient.run(instanceName, zookeepers, username, password, table, filename);
					break;
				case "indexWrite":
					IndexIngestClient.run(instanceName, zookeepers, username, password, table, filename);
					break;
				case "authWrite":
					AuthIngestClient.run(instanceName, zookeepers, username, password, table, filename);
					break;
        case "iterWrite":
          IteratorClient.run(instanceName, zookeepers, username, password, table, filename);
          break;
				case "read":
					ScanClient.run(instanceName, zookeepers, username, password, table, row, columnFamily, columnQualifier);
					break;
				case "indexRead":
					IndexScanClient.run(instanceName, zookeepers, username, password, table, startValue, endValue);
					break;
				case "authRead":
					AuthScanClient.run(instanceName, zookeepers, username, password, table, row, columnFamily, columnQualifier);
					break;
				case "update":
					UpdateDeleteClient.run(instanceName, zookeepers, username, password, table, row, columnFamily, columnQualifier, newValue, false);
					break;
				case "delete":
					UpdateDeleteClient.run(instanceName, zookeepers, username, password, table, row, columnFamily, columnQualifier, newValue, true);
					break;
        case "mapred":
          MapReduceClient.execute(instanceName, zookeepers, username, password, table, outputTable);
          break;
        case "spark":
          SparkClient.run(instanceName, zookeepers, username, password, table);
          break;
        case "replicate":
          ReplicationDataGenerator.run(instanceName, zookeepers, username, password, table);
          break;
				default:
					HelpFormatter formatter = new HelpFormatter();
					formatter.printHelp("", options);
			}
		} catch (ParseException pex) {
			System.out.println(pex.getMessage());
      
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("TrainingCode [write | indexWrite | authWrite | iterWrite | read | indexRead | authRead | update | delete | mapred | spark | replicate]", options);
		}
	}
}
