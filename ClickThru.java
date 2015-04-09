import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.json.simple.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.lang.*;

public class ClickThru extends Configured implements Tool {

	public static void main(String[]  args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new ClickThru(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Error: Wrong number of parameters");
      		System.err.println("Expected: [in] [out]");
      		System.exit(1);
		}

		Configuration conf = getConf();

		Job job = new Job(conf,"ClickThrough Rate");
		job.setJarByClass(ClickThru.class);

		job.setMapperClass(ClickThru.IdentityMapper.class);
		job.setReducerClass(ClickThru.IdentityReducer.class);

		Path[] paths = new Path[args.length-1];

		for(int i = 0; i<args.length-1; i++){
      		paths[i] = new Path(args[i]);
    	}

    	FileInputFormat.setInputPaths(job, paths);
    	FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));

    	job.setOutputKeyClass(Text.class);
    	job.setOutputValeClass(Text.class);

    	return job.waitForCompletion(true) ? 0 : 1;
	}

	//map [impression ID, url, adID] key to either 0 (impressions collection) or 1 (clicks collection) val
	public static class ImpressionsMapper extends Mapper<LongWritable,Text,Text,Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void map(LongWritable key, Text val, Context context) 
							throws IOException, InterruptedException {

			JSONParser parser = new JSONParser();
			StringBuilder parsedData = new StringBuilder();

			String impressionId;
			String referrer;
			String adId;
			String behavior;

			try {
				JSONObject jsnObj = new JSONObject(val.toString());
				impressionsId = (String)jsnObj.get("impressionId");
				outputKey.set(impressionId);
				if(jsnObj.containsKey("referrer")) {
					try {
						referrer = (String)jsnObj.get("referrer");
						adId = (String).jsnObj.get("adId");
						behavior = "0";
						parsedData.append(referrer);
						parsedData.append("\x1f");
						parsedData.append(adId);
						parsedData.append("\x1f");
						parsedData.append(behavior);
						outputValue.set(parsedData.toString());
						context.write(outputKey,outputValue);
					} catch (JSONException e) {
						e.printStackTrace();
					}
				} else {
					behavior = "1";
					outputValue.set(behavior);
					context.write(outputKey,outputValue);
				}
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
	}
	
	//add values of [impression ID, url, adID]: 0 +1 = 1 if clicked through, or just 0 if only impression
	public static class ImpressionsReducer extends Reducer<Text,Text,Text,Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context contect)) 
							throws IOException, InterruptedException {

				
				/*
				impTotal = 0
				String url
				String adID
				iterate over values
					currentVal = split value by "\x1f"
					if length of currentVal == 1
						impTotal = 1
					else
						url = currentVal[0]
						adID = currentVal[1]
				output:
				String key = url + "\x1f" + adID
				String val = impTotal (or can this be an integer?)
				*/
		}

	}
	//INPUT: [url, adID] -> 0 or 1
	//OUTPUT: [url, adID] -> 0 or 1
	public static class ClicksMapper extends Mapper<LongWritable,Text,Text,Text> {

		@Override
		public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {


		}

	}

	//INPUT: [url, adID] -> vals[0,1,1...]
	//OUTPUT: [url, adID] -> 1 count / totalCount = CTR
	public static class ClicksReducer extends Reducer<Text,Text,Text,Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context contect)) 
							throws IOException, InterruptedException {
				int totalImpressions = 0
				int totalClicks = 0
				for(Text value : values){
					totalImpressions++
					totalClicks += Strings.Atoi(value.toString())
				}
				Text clickThroughRate = new Text();
	       		clickThroughRate.set(Strings.Itoa(totalClicks/totalImpressions));
	        	context.write(key, documentList);
		}

	}

}