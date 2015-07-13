package com.ukon.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class WordCount
{
	public static void main(String[] args) throws IOException
	{
		JobConf jobConf = new JobConf(WordCount.class);
		jobConf.setJobName("wordCount");

		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);

		jobConf.setMapperClass(WordCountMapper.class);
		jobConf.setCombinerClass(WordCountReducer.class);
		jobConf.setReducerClass(WordCountReducer.class);

		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);


		//Single input file
		FileInputFormat.setInputPaths(jobConf, new Path("wordcount-input-set.txt"));
		FileOutputFormat.setOutputPath(jobConf, new Path("wordcount-output-set.txt"));

		JobClient.runJob(jobConf);
	}

}
