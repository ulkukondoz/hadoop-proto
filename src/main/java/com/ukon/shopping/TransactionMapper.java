package com.ukon.shopping;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransactionMapper extends Mapper<LongWritable, Text, Text, Text>
{

    public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
        String[] lines = text.toString().split(",");
        String uid = lines[2];
        String product = lines[1];

        System.out.println("--- TRAN MAP : uid : " + uid + " product : " + product);

//        context.write(new Text(uid), new Text("pid" + product));
        context.write(new Text(uid), new Text("pid" + product));

    }
}
