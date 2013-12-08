package lv.timeklis.twitterscraper;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import twitter4j.GeoLocation;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterScraperMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, NullWritable> {
	
	private Twitter twitter;
	
	@Override
	public void configure(JobConf jobConf) {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
				.setOAuthConsumerKey(jobConf.get("OAuthConsumerKey"))
				.setOAuthConsumerSecret(jobConf.get("OAuthConsumerSecret"))
				.setOAuthAccessToken(jobConf.get("OAuthAccessToken"))
				.setOAuthAccessTokenSecret(jobConf.get("OAuthAccessTokenSecret"));
		TwitterFactory tf = new TwitterFactory(cb.build());
		twitter = tf.getInstance();
	}

	private final NullWritable nullResult = NullWritable.get();
	private Text textResult = new Text();

	public String escape(String str) {

		str = str.replace("\"", "\"\"");
		str = str.replace("\n", " ");
		str = "\"" + str + "\"";
		return str;
	}

	@Override
	public void map(LongWritable key, Text search, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
		
		try {
			String searchQuery = search.toString();
			if(searchQuery.length() < 2) {
				return;
			}
			Query query;
			if(searchQuery.equals("location-latvia")) {
				query = new Query();
				GeoLocation location = new GeoLocation(56.950404d,24.11823d);
				query.setGeoCode(location, 150, "km");
			}
			else {
				query = new Query(searchQuery);
			}
			query.setLang("lv");
			query.count(100);
		
			QueryResult result = twitter.search(query);
			for (Status tweet : result.getTweets()) {
				
				GeoLocation location = tweet.getGeoLocation();
				String result_location = new String();
				if(location != null) {
					result_location = location.toString();
				}
				
				String record = tweet.getId()
						+ "," + tweet.getCreatedAt().toString()
						+ "," + result_location
						+ "," + escape(tweet.getIsoLanguageCode() == null ? "":tweet.getIsoLanguageCode())
						+ "," + tweet.getFavoriteCount()
						+ "," + tweet.getInReplyToStatusId()
						+ "," + tweet.getInReplyToUserId()
						+ "," + tweet.getContributors().toString()
						+ "," + escape(tweet.getPlace()== null?"":tweet.getPlace().toString())
						+ "," + tweet.getRetweetCount()
						+ "," + tweet.getRetweetedStatus()
						+ "," + escape(tweet.getText())
						+ "," + escape(tweet.getUser().getName())
						+ "," + escape(tweet.getUser().getLocation() == null?"":tweet.getUser().getLocation())
						+ "," + escape(tweet.getUser().getLang() == null?"":tweet.getUser().getLang());
				textResult.set(record);
				output.collect(textResult, nullResult);
			}
			
			
		} catch (TwitterException ex) {
			Logger.getLogger(TwitterScraperMapper.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}