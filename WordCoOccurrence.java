package edu.bufflo.sem2.dic.lab4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCoOccurrence {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word;
		private Text out_word = new Text();
		private List<Text> wordList = new ArrayList<Text>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\s+");
			if (tokens.length > 1) {
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
						//System.out.println("wtext words == " + f + " " + s);
						StringBuilder keyStr = new StringBuilder();
						keyStr.append(f);
						keyStr.append(",");
						keyStr.append(s);
						out_word.set(keyStr.toString());
						context.write(out_word, one);
					}
				}
			}

		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
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
		Job job = new Job(conf, "word coOccurrence");
		job.setJarByClass(WordCoOccurrence.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		// job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(IntWritable.class);
		// addJarToDistributedCache(Pair.class, conf);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
