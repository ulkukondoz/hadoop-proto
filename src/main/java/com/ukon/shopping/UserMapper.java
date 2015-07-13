package com.ukon.shopping;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserMapper extends Mapper<LongWritable, Text, Text, Text>

{
	public void map(LongWritable longWritable, Text text, Context context)
            throws IOException, InterruptedException {
		String[] lines = text.toString().split(",");
		String uid = lines[0];
		String location = lines[3];

        System.out.println("--- USER MAP : uid : " + uid + " location : " + location);
        context.write(new Text(uid), new Text("lid" + location));
    }
}
