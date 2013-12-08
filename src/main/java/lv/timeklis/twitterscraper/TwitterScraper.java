package lv.timeklis.twitterscraper;

import java.io.File;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;


public class TwitterScraper {

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf();
		conf.setJobName("WarcCounter");

		// This line specifies the jar Hadoop should use to run the mapper and
		// reducer by telling it a class that’s inside it
		conf.setJarByClass(TwitterScraper.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(NullWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);

		conf.setMapperClass(TwitterScraperMapper.class);
		
		// twitter api access
		conf.set("OAuthConsumerKey", "");
		conf.set("OAuthConsumerSecret", "");
		conf.set("OAuthAccessToken", "");
		conf.set("OAuthAccessTokenSecret", "");

		// KeyValueTextInputFormat treats each line as an input record,
		// and splits the line by the tab character to separate it into key and value
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// Run this job locally
		conf.set("mapreduce.jobtracker.address", "local");
		conf.set("fs.defaultFS", "file:///");

		String output_dir = "/home/martins/hadoop/output/";
		deleteLocalDir(new File(output_dir));

		FileInputFormat.setInputPaths(conf, new Path("/home/martins/hadoop/input/twitter"));
		FileOutputFormat.setOutputPath(conf, new Path(output_dir));

		JobClient.runJob(conf);
	}

	private static boolean deleteLocalDir(File directory) {
		if (directory.exists()) {
			File[] files = directory.listFiles();
			if (null != files) {
				for (int i = 0; i < files.length; i++) {
					if (files[i].isDirectory()) {
						deleteLocalDir(files[i]);
					} else {
						files[i].delete();
					}
				}
			}
		}
		return (directory.delete());
	}
}
