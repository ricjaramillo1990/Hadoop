import java.io.BufferedReader;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.HashMap;
/**
 * Student ID: 2647579
 *
 */

public class SentimentWords 
{
	
	
	// The mapper class
	public static class SentimentMapper extends Mapper<Object, Text, Text, Object>
	{				
		String[] excludedWords = null;
	    /**
	     * Processes a line that is passed to it by writing a key/value pair to the context. 
	     * 
	     * @param index 	A link to the file index
	     * @param value 	The line from the file
	     * @param context 	The Hadoop context object
	     */
	    public void map(Object index, Text value, Context context) throws IOException, InterruptedException 
	    {	    	
	    	// Split the text data into it's separate columns using Tab as the delimeter
			String[] columns = value.toString().split("\t");
			// Structure of every line is composed by 3 parts: types (MOVIES, PRODUCT and
			// RESTAURANT), words (all possible words), sentimient (0 or 1)
			String types = columns[0];
			// with regex all characters different from number or letter will be removed.
			// Double spaces are set to single space
			// every word is saved into array
			String[] words = columns[1].replaceAll("[^a-zA-Z0-9\\s+]", "").replaceAll("  ", " ").split(" ");
			String sentimient = columns[2];

			// Every word in 'words' is converted to lower case and excluded from the
			// exclude.txt file if match
			ArrayList<String> palabra = new ArrayList<String>();
			for (String word : words) {
				int cont = 0;
				String pal = word.toLowerCase().replaceAll(",,", ",");
				// Exclude validatior
				for (String name : excludedWords) {
					if (pal.equals(name) || pal.equals("") || pal.equals(null)) {
						cont = 0;
					} else {
						cont = cont + 1;
					}
					if (cont == excludedWords.length) {
						palabra.add(pal + "=1");
						cont = 0;
					}
				}

			}
			// Every word at this point will have the count equal to 1, even if there are
			// more than 1 words in the same line.
			// Combiner will group similar words
			// 'types' and 'sentiment' will be stored in the first parameter of the mapper
			// Mapper output example: Movie 1 word1=1, word2=2,.... wordn=1
			context.write(new Text(types + " " + sentimient),
					new Text(palabra.toString().replaceAll(",,", ",").replaceAll("[\\{\\}\\[\\]]", "")));		    	
	    	  	
	    	
	    }	
	    
	    public void setup(Context context) 
	   	{
	    	try
	    	{
	    		// exclude.txt will be uploaded via cache
	    		URI[] cacheFiles = context.getCacheFiles();
	    		String[] fname=cacheFiles[0].toString().split("#");
	    		// internal HDFS filename reference.
	    		BufferedReader br = new BufferedReader(new FileReader(fname[1]));
	    		String cache_line=br.readLine();
	    		// All words will be force to be lower case
	    		excludedWords = cache_line.toLowerCase().split(","); 
	    		//System.out.println(cache_line);
	    		br.close();
	    		  		  		
	    	}
	    	catch (Exception e)
	    	{
	    		System.err.println("Problems setting up mapper: " + e);
	    	}
	    
	    }
	    
//	    public void cleanup(Context context)
//	    {
//	    	System.out.println("Cleaning up mapper");
//	    }
	}

	public static class SentimentCombiner extends Reducer<Text,Text,Text,Text> 
	{	    
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	    {			
			//Combiner will attempt to sum lines with more than 1 occurrence
			Map<String, Integer> merging = new HashMap<String, Integer>();
			Set<String> keys = merging.keySet();
			Iterator<Text> iter = values.iterator();

			// every value is splitted by ',' then by '='
			// through a loop it will add values and save to an hashmap
			while (iter.hasNext()) {
				Text t = iter.next();
				String[] sep_by_comma = t.toString().trim().replaceAll("[\\{\\}\\[\\]]", "").split(",");
				for (String r : sep_by_comma) {
					String[] sep_by_equal = r.split("=");
					String x = sep_by_equal[0].trim();
					if (sep_by_equal.length >= 2) {
						Integer y = Integer.valueOf(sep_by_equal[1].trim());
						// If the value exists the loop gets the current value and adds the new one
						if (merging.containsKey(x)) {
							for (String keyq : keys) {
								if (keyq.equals(x)) {
									Integer valor2 = merging.get(x);
									int valores = y + valor2;
									merging.put(x, valores);
								}
							}
						} else {
							// If the value is new in the map will be added
							merging.put(x, y);
						}
					}
				}
			}
	    	context.write(new Text (key), new Text (merging.toString()));
	    		
	    }
//	    public void setup(Context context)
//	    {
//	    	System.out.println("Setting up combiner");
//	    }
	    
//	    public void cleanup(Context context)
//	    {
//	    	System.out.println("Cleaning up combiner");
//	    }
	    
	}
	
	// The Reducer class
	public static class SentimentReducer extends Reducer<Text,Text,Text,Text> 
	{	
		
		
		/**
		 * Reduces multiple data values for a given key into a single output for that key
		 * 
		 * @param key 		The key that this particular reduce call will be operating on
		 * @param values 	An array of values associated with the given key
		 * @param context	The Hadoop context object
		 */
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	    {   
	    	//If for some reason combiner doesnt run, reducer will be ready to sum values
	    	//therefore, it can receive inputs from mapper or combiner without problems
	    	Map<String, Integer> merging1 = new HashMap<String, Integer>();
	    	Set<String> keys = merging1.keySet();	    		    	  
	    	Iterator<Text> ite = values.iterator();
	    	
	    	//same logic as the combiner
			while (ite.hasNext()) {
				Text t = ite.next();
				String[] sep_by_comma = t.toString().trim().replaceAll("[\\{\\}\\[\\]]", "").split(",");
				for (String r : sep_by_comma) {
					String[] sep_by_equal = r.split("=");
					String x = sep_by_equal[0].trim();
					if (sep_by_equal.length >= 2) {
						Integer y = Integer.valueOf(sep_by_equal[1].trim());
						// If the value exists the loop gets the current value and adds the new one
						if (merging1.containsKey(x)) {
							for (String keyq : keys) {
								if (keyq.equals(x)) {
									Integer valor2 = merging1.get(x);
									int valores = y + valor2;
									merging1.put(x, valores);
								}
							}
						} else {
							// If the value is new in the map will be added
							merging1.put(x, y);
						}
					}
				}
			}
	    	
	    	///with all the values grouped, we proceed to sort the values from greater to lower
	        // limit(5) will only retrieve the top 5 words
			Map<String, Integer> sortedByCount = merging1.entrySet().stream()
					.sorted((Map.Entry.<String, Integer>comparingByValue().reversed())).limit(5).collect(Collectors
							.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
			
			//Only keys from the map will be printed
			String finals = sortedByCount.keySet().toString().replaceAll("[\\{\\}\\[\\]\\,]", "");
			context.write(new Text(key), new Text(finals));
		}
	    
	    
//	    public void setup(Context context)
//	    {
//	    	System.out.println("Setting up reducer");
//	    }
	    
//	    public void cleanup(Context context)
//	    {
//	    	System.out.println("Cleaning up reducer");
//	    }
	    
	}

	  /**
	   * main program that will be run on Hadoop, including configuration setup. You may 
	   * want to comment out this method while testing your code on Mochadoop.
	   * 
	   * @param args		Command line arguments
	   */
	  public static void main(String[] args) throws Exception 
	  {		
	    Configuration conf = new Configuration();
	    ///parameters...
	    //conf.set("myParamName",args[2]);
	    
	    Job job = Job.getInstance(conf, "Product Sentiment Words");
	    
	    ///// cofiguration to expect the cache file via arguments
	    job.addCacheFile(new URI(args[2]));	    
	    
	    job.setJarByClass(SentimentWords.class);
	    
	    // Set mapper class to SentimentMapper defined above
	    job.setMapperClass(SentimentMapper.class);
	    
	    // Set combine class to SentimentCombiner defined above
	    job.setCombinerClass(SentimentCombiner.class);
	    
	    // Set reduce class to SentimentReducer defined above
	    job.setReducerClass(SentimentReducer.class);
	    
	    // Class of output key is Text
	    job.setOutputKeyClass(Text.class);		
	    
	    // Class of output value is Text
	    job.setOutputValueClass(Text.class);
	    
	    // Input path is first argument when program called
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    
	    // Output path is second argument when program called
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));	
	    
	    // waitForCompletion submits the job and waits for it to complete, 
	    // parameter is verbose. Returns true if job succeeds.
	    System.exit(job.waitForCompletion(true) ? 0 : 1);		
	  }
}



