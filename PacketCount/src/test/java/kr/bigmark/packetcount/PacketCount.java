package kr.bigmark.packetcount;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import p3.hadoop.mapreduce.lib.input.PcapInputFormat;
import p3.tcphttp.analyzer.lib.PacketStatsWritable;

public class PacketCount {
 
 private static final int ONEDAYINSEC = 432000;
        
  public static class Map extends Mapper<LongWritable, BytesWritable, Text, IntWritable> {
  
 private final int MIN_PKT_SIZE = 42;
    private final static IntWritable one = new IntWritable(1);
        
    public void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
     
    	if(value.getBytes().length < MIN_PKT_SIZE) 
    		return;
 
    	PacketStatsWritable ps = new PacketStatsWritable();
 
		 if(ps.parse(value.getBytes())) 
			 context.write(new Text(ps.getSrc_ip()), one); 
		 }
  } 
         
  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
 
     public void reduce(Text key, Iterable<IntWritable> values, Context context) 
       throws IOException, InterruptedException {
         int sum = 0;
         for (IntWritable val : values) {
             sum += val.get();
         }
         context.write(key, new IntWritable(sum));
     }
  }

          
 public static void main(String[] args) throws Exception {
	 Configuration conf = new Configuration();
	 Job job = new Job(conf, "wordcount");
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(IntWritable.class);
	 job.setMapperClass(Map.class);
	 job.setReducerClass(Reduce.class);
	 job.setInputFormatClass(PcapInputFormat.class);
	 job.setOutputFormatClass(TextOutputFormat.class);
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 job.waitForCompletion(true); 
 }
}