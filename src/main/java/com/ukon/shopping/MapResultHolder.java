package com.ukon.shopping;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapResultHolder implements WritableComparable<MapResultHolder> {

    private Text key;
    private Text value;

    public MapResultHolder(Text key, Text value){
        this.key = key;
        this.value = value;
    }

    public MapResultHolder(String key, String value){
        new MapResultHolder(new Text(key), new Text(value));
    }

    public MapResultHolder(){
    }

    public Text getKey() {
        return key;
    }

    public Text getValue() {
        return value;
    }

    public void setKey(Text key) {
        System.out.println("--- setting key  : " + key);
        this.key = key;
    }

    public void setValue(Text value) {
        System.out.println("--- setting value  : " + value);
        this.value = value;
    }

    public int compareTo(MapResultHolder o) {
        return key.compareTo(o.getKey());
    }

    public void write(DataOutput dataOutput) throws IOException {
        key.write(dataOutput);
        value.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        key.readFields(dataInput);
        value.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MapResultHolder)) return false;

        MapResultHolder that = (MapResultHolder) o;

        if (!key.equals(that.key)) return false;
        return value.equals(that.value);

    }

//    @Override
//    public int hashCode() {
//        int result = key.hashCode();
//        result = 31 * result + value.hashCode();
//        return result;
//    }

    @Override
    public String toString() {
        return "MapResultHolder{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
