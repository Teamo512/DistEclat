package distEclat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static org.apache.hadoop.mapreduce.lib.output.MultipleOutputs.addNamedOutput;

public class DistEclatDriver extends Configured implements Tool {

	private String[] moreParas;
	private static String dataBaseName;
	private static double relativeMinSupport;
	private String input_file;
	private String output_dir;
	private static int dataSize;
	private static int numMappers;
	private static int childJavaOpts;
	private static long minsup;
	private static int prefix_length;
	public static final String NUMBER_OF_LINES_KEY = "number_of_lines_read";
	// Whether to write the actually write the results. If set to false, only
	// the timings will be reported. Default is true.
	private static boolean write_sets = false;
	
	//Count the time of each stage  20170227  -jiang
	private static ArrayList<Double> eachLevelRunningTime = new ArrayList<Double>();
	private static ArrayList<Long> eachLeveltidsetItemsetsNum = new ArrayList<Long>();  //count the number of each frequentPattern   20170416
	
	// output files first MapReduce cycle
	public static final String OSingletonsDistribution = "singletonsDistribution";
	public static final String OSingletonsOrder = "singletonsOrder";
	public static final String OSingletonsTids = "singletonsTids";

	// output files second MapReduce cycle
	public static final String OFises = "fises";
	public static final String OPrefixesDistribution = "prefixesDistribution";
	public static final String OPrefixesGroups = "prefixesGroups";

	
	
	// output files third MapReduce cycle
	private static final String OFis = "fis";

	// default extension for output file of first reducer
	public static final String rExt = "-r-00000";

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.setLong("minsup", minsup);
		conf.setInt("numMappers", numMappers);
		conf.setInt("prefix_length", prefix_length);
		
		conf.set("mapreduce.task.timeout", "6000000"); 
		
		conf.set("mapreduce.map.java.opts", "-Xmx"+childJavaOpts+"M");
		conf.set("mapreduce.reduce.java.opts", "-Xmx"+2*childJavaOpts+"M");

		for( int k = 0; k < moreParas.length && moreParas.length >= 2; k+=2) {
			conf.set( moreParas[ k ], moreParas[ k+1 ] );			
		}
		
		try{
			String tmpDir1 = output_dir + "/" + "tmp1" + "/";
			String tmpDir2 = output_dir + "/" + "prefixes" + "/";

			long start = System.currentTimeMillis();
			Tools.cleanDirs(new String[] { output_dir, tmpDir1, tmpDir2 });

			startItemReading(input_file, tmpDir1, conf);
			startPrefixComputation(tmpDir1, tmpDir2, conf);
			startMining(tmpDir2, output_dir, conf);
			long end = System.currentTimeMillis();

			System.out.println("[Eclat]: Total time: " + (end - start) / 1000.0 + "s");
			saveResult((end - start) / 1000.0);
		}catch(Exception e){
	    	e.printStackTrace();
	    	File resultFile = new File("DistEclat_" + dataBaseName + "_ResultOut");
			BufferedWriter br  = new BufferedWriter(new FileWriter(resultFile, true));  // true means appending content to the file //here create a non-existed file
			br.write("DistEclat Exception occurs at minimumSupport(relative) "  + relativeMinSupport);
			br.write("\n");
			br.flush();
			br.close();
	    }
		return 0;

	}

	//one phase
	private static void startItemReading(String inputFile, String outputFile, Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		System.out.println("[ItemReading]: input: " + inputFile + ", output: " + outputFile);

		// Job job = new Job(conf, "Read Singletons");
		Job job = Job.getInstance(conf, "Read Singletons");
		job.setJarByClass(DistEclatDriver.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntArrayWritable.class);

		job.setMapperClass(ItemReaderMapper.class);
		job.setReducerClass(ItemReaderReducer.class);

		job.setInputFormatClass(SplitByNumberOfMappersTextInputFormat.class); //根据map个数来划分split个数
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(outputFile));

		addNamedOutput(job, OSingletonsDistribution, TextOutputFormat.class, Text.class, Text.class);

		addNamedOutput(job, OSingletonsOrder, TextOutputFormat.class, Text.class, Text.class);

		addNamedOutput(job, OSingletonsTids, SequenceFileOutputFormat.class, Text.class, IntArrayWritable.class);

		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		System.out.println("Job Item Reading took " + (end - start) / 1000.0 + "s");
		eachLevelRunningTime.add((end - start) / 1000.0);
		eachLeveltidsetItemsetsNum.add(job.getCounters().findCounter(Counter.FrePattern).getValue());
	}

	private static void startPrefixComputation(String inputDir, String outputDir, Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

		
		String inputFile = inputDir + "/" + OSingletonsDistribution + rExt;
		String outputFileFises = OFises;
		String outputFilePrefixes = OPrefixesDistribution;
		
		String singletonsOrderFile = inputDir + "/" + OSingletonsOrder + rExt;
		String singletonsTidsFile = inputDir + "/" + OSingletonsTids + rExt;

		System.out.println("[PrefixComputation]: input: " + inputFile + ", output fises: " + outputFileFises + ", output prefixes: " + outputFilePrefixes);

		Job job = Job.getInstance(conf, "Compute Prefixes");
		job.setJarByClass(DistEclatDriver.class);

		job.addCacheFile(URI.create(singletonsOrderFile));
		job.addCacheFile(URI.create(singletonsTidsFile));

		FileInputFormat.addInputPath(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntArrayWritable.class);

		job.setMapperClass(PrefixComputerMapper.class);
		job.setReducerClass(PrefixComputerReducer.class);

		job.setInputFormatClass(NLineInputFormat.class);

		job.setNumReduceTasks(1);

		job.setOutputKeyClass(IntArrayWritable.class);
		job.setOutputValueClass(IntArrayWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		System.out.println("[PartitionPrefixes]: Took " + (end - start) / 1000.0 + "s");
		eachLevelRunningTime.add((end - start) / 1000.0);
	}

	private static void startMining(String inputDir, String outputDir, Configuration conf)
			throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

		String inputFilesDir = inputDir;
		String outputFile = outputDir + "/" + OFis;
		System.out.println("[StartMining]: input: " + inputFilesDir + ", output: " + outputFile);

		Job job = Job.getInstance(conf, "Start Mining");
		job.setJarByClass(DistEclatDriver.class);

		job.setOutputKeyClass(Text.class);

		if (write_sets) {
			job.setOutputValueClass(Text.class);
			job.setMapperClass(EclatMinerMapper.class);
			job.setReducerClass(EclatMinerReducer.class);
		} else {
			job.setOutputValueClass(LongWritable.class);
			job.setMapperClass(EclatMinerMapperSetCount.class);
			job.setReducerClass(EclatMinerReducerSetCount.class);
		}

		job.setInputFormatClass(NoSplitSequenceFileInputFormat.class);

		List<Path> inputPaths = new ArrayList<Path>();

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] listStatus = fs.globStatus(new Path(inputFilesDir + "bucket*"));
		for (FileStatus fstat : listStatus) {
			inputPaths.add(fstat.getPath());
		}

		FileInputFormat.setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
		FileOutputFormat.setOutputPath(job, new Path(outputFile));

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(1);

		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		System.out.println("[Mining]: Took " + (end - start) / 1000.0 + "s");
		eachLevelRunningTime.add((end - start) / 1000.0);
		eachLeveltidsetItemsetsNum.add(job.getCounters().findCounter(Counter.FrePattern).getValue());
		fs.close();

	}

	public static void saveResult(double totalTime){
		try{
			BufferedWriter br = null;
			double TotalJobRunningTime = 0;
			long TotalFrePattern = 0;
			for(int i=0; i<3; i++){
				TotalJobRunningTime += eachLevelRunningTime.get(i);
			}
			
			for(int i=0; i<eachLeveltidsetItemsetsNum.size(); i++)
				TotalFrePattern +=  eachLeveltidsetItemsetsNum.get(i);
			
			File resultFile = new File("DistEclat_" + dataBaseName + "_ResultOut");
			if(!resultFile.exists()){
				br  = new BufferedWriter(new FileWriter(resultFile, true));
				br.write("algorithmName" + "\t" + "datasetName" + "\t" + "DBSize" + "\t" + "minSuppPercentage(relative)" + "\t" + "minSupp(absolute)" +  "\t" + "childJavaOpts" + "\t" + "numMappers" + "\t" + "prefixLength" + "\t" + "TotalFrequentPattern" + "\t" + "TotalTime" + "\t" + "TotalJobTime"  + "\t");
				
				for( int i = 0; i<3; i++)  {
					br.write("Level_" + (i+1) + "_JobRunningTime" + "\t");
				}
						
				br.write("\n");
			}else{
				br  = new BufferedWriter(new FileWriter(resultFile, true));
			}
			
			br.write("DistEclat" + "\t" + dataBaseName + "\t" + dataSize + "\t" + relativeMinSupport * 100.0 + "\t" + minsup + "\t" + childJavaOpts + "\t" + numMappers + "\t" + prefix_length + "\t" + TotalFrePattern + "\t" + totalTime + "\t" +TotalJobRunningTime + "\t");
			for(int i=0; i<3; i++) {	
				br.write(eachLevelRunningTime.get(i) + "\t"); 	
			}
			
			br.write("\n");
			br.flush();
			br.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public DistEclatDriver(String[] args) {
		int numFixedParas = 8; // datasetName, minsup(relative), inpu_file, output_file, 
		                       // datasize, childJavaOpts, nummappers, prefix_length.
		int numMoreParas = args.length - numFixedParas;
		if (args.length < numFixedParas || numMoreParas % 2 != 0) {
			System.out.println("The Number of the input parameters is Wrong!!");
			System.exit(1);
		} else {
			if (numMoreParas > 0) {
				moreParas = new String[numMoreParas];
				for (int k = 0; k < numMoreParas; k++) {
					moreParas[k] = args[numFixedParas + k];
				}
			} else {
				moreParas = new String[1];
			}
		}
		dataBaseName = args[0];
		relativeMinSupport = Double.parseDouble(args[1]);
		input_file = args[2];
		output_dir = args[3];
		dataSize = Integer.parseInt(args[4]);
		minsup = (long)Math.ceil(relativeMinSupport * dataSize);
		childJavaOpts = Integer.parseInt(args[5]);
		numMappers = Integer.parseInt(args[6]);
		prefix_length = Integer.parseInt(args[7]);
	}


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new DistEclatDriver(args), args);
		System.out.println(res);
	}
}
