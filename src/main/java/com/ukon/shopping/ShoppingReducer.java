package com.ukon.shopping;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ShoppingReducer extends Reducer<Text, Text, Text, Text>
{

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        System.out.println("======== REDUCER =========");

        Text location = new Text("UNKNOWN - LOCATION");
        Text product = new Text("UNKNOWN - PROD");

        for (Text value : values) {
            System.out.println("<K- V> " + key + " " + value );
            if(value.toString().contains("lid")) {
                location = new Text(value);
                continue;
            }

            if(value.toString().contains("pid")) {
                product = new Text(value);
            }

            context.write(product, location);
        }
    }
}

