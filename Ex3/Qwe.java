package Round;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class Qwe {
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text>{
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String pattern ="(\\d+.\\d+.\\d+.\\d+)(.+)";
			Pattern p=Pattern.compile(pattern);
			Matcher m = p.matcher(value.toString());
				if(m.find()){
					output.collect(new Text(m.group(1)), new Text(m.group(2)));
				}
			}			
	}
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
		//private static int ipCount = 0;
		public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			while(value.hasNext()){
				output.collect(new Text(key), new Text(value.next()));
			}
		}
	}
	public static void main(String[] args) throws Exception {
		
		JobConf conf = new JobConf(Qwe.class);
		conf.setJobName("Qwe");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setNumReduceTasks(1);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}

