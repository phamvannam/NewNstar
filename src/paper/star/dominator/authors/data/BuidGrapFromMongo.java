package paper.star.dominator.authors.data;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;
public class BuidGrapFromMongo extends MongoTool{

	private static DirectedGraph<Integer,Integer> graph=new DefaultDirectedGraph<>(Integer.class);


	public static class MedlineKeywordMaper1 extends Mapper <Object, BSONObject,Text, Text>  
	implements org.apache.hadoop.mapred.Mapper<Object, BSONWritable, Text, IntWritable> {
		@Override
		public void map(Object key, BSONObject val,
				Context context)

						throws IOException, InterruptedException {
			int _id=(int)val.get("_id");
			Text txout=new Text();
			if (val.containsField("_id")) {
				int value= _id;
				txout.set(value+"");	
				if (txout.getLength()>0) {
					context.write( txout,new Text(val.toString()));
				}
			}
		}

		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub

		}
		@Override
		public void map(Object arg0, BSONWritable arg1,
				OutputCollector<Text, IntWritable> arg2, Reporter arg3)
						throws IOException {
			// TODO Auto-generated method stub

		}

	}
	private static Integer indx=8000000;
	public static class MedlineKeywordReduce1 extends Reducer<Text, Text, NullWritable, MongoUpdateWritable>
	implements org.apache.hadoop.mapred.Reducer<Text, Text, NullWritable, MongoUpdateWritable> {
		HashMap<ObjectId,ArrayList<Object>> hashMap=new HashMap<ObjectId, ArrayList<Object>>();
		@Override
		public void reduce(final Text pKey, final Iterable<Text> pValues, final Context pContext) throws IOException, InterruptedException {
			BasicBSONObject query = new BasicBSONObject("_id", pKey.toString());
			ArrayList<String> devices = new ArrayList<String>();
			for (Text val : pValues) {
				try {
					JSONObject jsonObject=new JSONObject(val.toString());
					Integer integer=new Integer((int)jsonObject.get("_id"));
					graph.addVertex(integer);
					JSONArray jsonArray=(JSONArray) jsonObject.get("authors");
					for (int i = 0; i < jsonArray.length(); i++) {
						Integer ver=new Integer((int)jsonArray.get(i));
						graph.addVertex(ver);
						graph.addEdge(integer, ver,indx++);
					}
					Integer ver=new Integer((int)jsonObject.get("journal"));
					graph.addVertex(ver);
					graph.addEdge(integer,ver,integer);
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
			int a=1;
			if(a>1)
			{
				BasicBSONObject update = new BasicBSONObject("$pushAll", new BasicBSONObject("pubs", devices));
				pContext.write(null, new MongoUpdateWritable(query, update, true, false));
			}

		}
		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void reduce(Text arg0, Iterator<Text> arg1,
				OutputCollector<NullWritable, MongoUpdateWritable> arg2, Reporter arg3)
						throws IOException {

		}
	}

	public static DirectedGraph<Integer,Integer> getGraphx() {
		return graph;
	}


	public BuidGrapFromMongo() throws UnknownHostException {
		setConf(new Configuration());


		if (MongoTool.isMapRedV1()) {
			MapredMongoConfigUtil.setInputFormat(getConf(), com.mongodb.hadoop.mapred.MongoInputFormat.class);
			MapredMongoConfigUtil.setOutputFormat(getConf(), com.mongodb.hadoop.mapred.MongoOutputFormat.class);
		} else {
			MongoConfigUtil.setInputFormat(getConf(), MongoInputFormat.class);
			MongoConfigUtil.setOutputFormat(getConf(), MongoOutputFormat.class);
		}

		MongoConfigUtil.setInputURI(getConf(),"mongodb://localhost:27017/nstar.publications.pubsC3");
		MongoConfigUtil.setOutputURI(getConf(),"mongodb://localhost:27017/Data.cpublications.mconf");

		MongoConfigUtil.setMapper(getConf(), MedlineKeywordMaper1.class);
		MongoConfigUtil.setReducer(getConf(), MedlineKeywordReduce1.class);

		MongoConfigUtil.setMapperOutputKey(getConf(), Text.class);
		MongoConfigUtil.setMapperOutputValue(getConf(), Text.class);

		MongoConfigUtil.setOutputKey(getConf(), IntWritable.class);
		MongoConfigUtil.setOutputValue(getConf(), BSONWritable.class);

	}

//	public static void main(final String[] pArgs) throws Exception {
//		System.exit(ToolRunner.run(new MapReduceSearch(), null));
//	}
}