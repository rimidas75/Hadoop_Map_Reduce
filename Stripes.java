package edu.bufflo.sem2.dic.lab4;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Stripes {

	public static class TokenizerMapper extends Mapper<Object,Text,Text,MapWritable> {

		private final static IntWritable one = new IntWritable(1);
		private MapWritable occurrenceMap = new MapWritable();
		private Text word;
		private Text out_word = new Text();
		private List<Text> wordList = new ArrayList<Text>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\s+");
			if (tokens.length > 1) {
				occurrenceMap.clear();
				wordList.clear();
				for (int i = 0; i < tokens.length; i++) {

					word = new Text();
					word.set(tokens[i]);
					//System.out.println(tokens[i]);
					wordList.add(word);
				}
			}

			for (int i = 0; i < wordList.size(); i++) {

				for (int j = 0; j < wordList.size(); j++)

				{

					// System.out.println("equals ===" + f.equals(s));
					if (i != j) {
						//System.out.println(i + " " + j);
						String f = wordList.get(i).toString();
						String s = wordList.get(j).toString();
						out_word.set(f);
						 if(occurrenceMap.containsKey(s)){
			                   IntWritable count = (IntWritable)occurrenceMap.get(s);
			                   count.set(count.get()+1);
			                }else{
			                	word.set(s);
			                   occurrenceMap.put(word,new IntWritable(1));
			                }								
					}
				}
				
				 context.write(out_word,occurrenceMap);
			}

		}
	}

	public static class StripesReducer extends  Reducer<Text, MapWritable, Text, Text> {
		//private IntWritable result = new IntWritable();
		private Text word  = new Text();
		private MapWritable reducedMap = new MapWritable();
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			reducedMap.clear();
	        for (MapWritable value : values) {
	            addAll(value);
	        }
	        StringBuilder s  = new StringBuilder();
	        Iterator<Entry<Writable, Writable>> mapIter = reducedMap.entrySet().iterator();
	        while(mapIter.hasNext())
	        {
	        	Entry<Writable, Writable> mapRow = mapIter.next();
	        	s.append(mapRow.getKey()+"|"+mapRow.getValue()+"|");
	        }
	        
	       
	        word.set(s.toString());
	        context.write(key, word);
		}
		
		 private void addAll(MapWritable mapWritable) {
		        Set<Writable> keys = mapWritable.keySet();
		        for (Writable key : keys) {
		            IntWritable fromCount = (IntWritable) mapWritable.get(key);
		            if (reducedMap.containsKey(key)) {
		                IntWritable count = (IntWritable) reducedMap.get(key);
		                count.set(count.get() + fromCount.get());
		            } else {
		            	reducedMap.put(key, fromCount);
		            }
		        }
		    }
		
		
		
	}

	/*
	 * private static void addJarToDistributedCache( Class classToAdd,
	 * Configuration conf) throws IOException {
	 * 
	 * // Retrieve jar file for class2Add String jar =
	 * classToAdd.getProtectionDomain(). getCodeSource().getLocation().
	 * getPath(); "/home/hadoop/Pair.jar"; File jarFile = new File(jar);
	 * 
	 * // Declare new HDFS location Path hdfsJar = new Path("~/lib/" +
	 * jarFile.getName());
	 * 
	 * // Mount HDFS FileSystem hdfs = FileSystem.get(conf);
	 * 
	 * // Copy (override) jar file to HDFS hdfs.copyFromLocalFile(false, true,
	 * new Path(jar), hdfsJar);
	 * 
	 * // Add jar to distributed classPath
	 * DistributedCache.addFileToClassPath(hdfsJar, conf, hdfs); }
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Stripes");
		job.setJarByClass(Stripes.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(StripesReducer.class);
		job.setReducerClass(StripesReducer.class);

		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(IntWritable.class);
		// addJarToDistributedCache(Pair.class, conf);
		job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
