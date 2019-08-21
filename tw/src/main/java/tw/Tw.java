package tw;

import java.io.File;
import java.util.ArrayList;
// imports for java
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
// imports for parser
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
//streaming imports
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;


public class Tw {
	public static void main(String[] args) throws Exception {
		File file = new File("/Users/oscarli/Desktop/banned.txt");
		Scanner sc = new Scanner(file);
		List<String> bannedWords = new ArrayList<String>();
		while (sc.hasNextLine()) {
			bannedWords
			.add(sc.nextLine());
		}

				final StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();
		
		Properties property = new Properties();
		property.setProperty("bootstrap.servers", "127.0.0.1:9092");
		
		DataStream<String> kafkaData = env.addSource(new FlinkKafkaConsumer011("twitterStream", new SimpleStringSchema(), property));
		DataStream<JsonNode> tweetJsonData = kafkaData.map(new TweetJsonParser());
		DataStream<JsonNode> filteredData = tweetJsonData.filter(new TextFilter()).filter(new LanguageFilter());
		DataStream<Tuple2<String, JsonNode>> tweetsBySource = filteredData.map(new ExtractSource());
		tweetsBySource.map(new ExtractHour())
		.keyBy(0, 1)
		.sum(2)
		.writeAsText("/Users/oscarli/Desktop/output.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);

		env.execute("Twitter Streaming");
	}

	public static class TweetJsonParser implements MapFunction<String, JsonNode> {

		public JsonNode map(String value) throws Exception {
			ObjectMapper jsonParser = new ObjectMapper();
			JsonNode node = jsonParser.readValue(value, JsonNode.class);
			return node;
		}
	}

	public static class LanguageFilter implements FilterFunction<JsonNode> {
		public boolean filter(JsonNode node) {
			boolean isEn = node.has("user") && node.get("user").has("lang")
					&& node.get("user").get("lang").asText().equals("en");
			return isEn;
		}
	}

	public static class ExtractSource implements MapFunction<JsonNode, Tuple2<String, JsonNode>> {
		public Tuple2<String, JsonNode> map(JsonNode node) {
			String source = "";
			if (node.has("source")) {
				String sourceHtml = node.get("source").asText().toLowerCase();
				if (sourceHtml.contains("iphone"))
					source = "Iphone";
				else if (sourceHtml.contains("ipad"))
					source = "Ipad";
				else if (sourceHtml.contains("mac"))
					source = "AppleMac";
				else if (sourceHtml.contains("android"))
					source = "Android";
				else if (sourceHtml.contains("BlackBerry"))
					source = "BlackBerry";
				else if (sourceHtml.contains("web"))
					source = "Web";
				else
					source = "Other";
			}
			return new Tuple2<String, JsonNode>(source, node);
		}
	}

	public static class ExtractHour implements MapFunction<Tuple2<String, JsonNode>, Tuple3<String, String, Integer>> {
		public Tuple3<String, String, Integer> map(Tuple2<String, JsonNode> value) {
			JsonNode node = value.f1;
			String timestamp = node.get("created_at").asText(); // Mon May 8 16:26:15 +0000 2019
			String hour = timestamp.split(" ")[3].split(":")[0] + "th hour";
			return new Tuple3<String, String, Integer>(value.f0, hour, 1);
		}
	}

	public static class FilterOutBannedWords implements FilterFunction<JsonNode> {
		private final List<String> filterKeyWords;

		public FilterOutBannedWords(List<String> filterKeyWords) {
			this.filterKeyWords = filterKeyWords;
		}

		public boolean filter(JsonNode node) {
			if (!node.has("text"))
				return false;
			String tweet = node.get("text").asText().toLowerCase();

			for (String word : filterKeyWords) {
				if (tweet.contains(word))
					return false; // exist in that prohibited list, remove
			}
			return true;
		}
	}
}





