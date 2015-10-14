package generatordata;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ToolRunner;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;

import org.apache.hadoop.conf.Configuration;
public class MapReduceObject extends MongoTool{



	public static class MedlineKeywordMaper1 extends Mapper <Object, BSONObject,Text, Text>  
	implements org.apache.hadoop.mapred.Mapper<Object, BSONWritable, Text, IntWritable> {
		@Override
		public void map(Object key, BSONObject val,
				Context context)

						throws IOException, InterruptedException {
			Text txout=new Text();
			//			if (val.containsField("_id")) {
			//				int value= _id;
			//				txout.set(value+"");	
			//				if (txout.getLength()>0) {
			//					context.write( txout,new Text(val.toString()));
			//				}
			//			}

			//			if (val.containsField("pubs")) {
			//			ArrayList<String> pubList=(ArrayList<String>) val.get("pubs");
			//			for (int i = 0; i < pubList.size(); i++) {
			//				String value= pubList.get(i).toString();
			//				txout.set(value);	
			//				if (txout.getLength()>0) {
			//					context.write( txout,new Text(_id));
			//				}	
			//			}
			//		}

			if (val.containsField("authors")) {
				@SuppressWarnings("unchecked")
				ArrayList<String> autList=(ArrayList<String>) val.get("authors");
				for (int i = 0; i < autList.size(); i++) {
					String value= autList.get(i).toString();
					txout.set(value);	
					if (txout.getLength()>0) {
						context.write( txout,new Text(val.toString()));
					}	
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
	public static class MedlineKeywordReduce1 extends Reducer<Text, Text, NullWritable, MongoUpdateWritable>
	implements org.apache.hadoop.mapred.Reducer<Text, Text, NullWritable, MongoUpdateWritable> {
		HashMap<ObjectId,ArrayList<Object>> hashMap=new HashMap<ObjectId, ArrayList<Object>>();
		@Override
		public void reduce(final Text pKey, final Iterable<Text> pValues, final Context pContext) throws IOException, InterruptedException {
			BasicBSONObject query = new BasicBSONObject("_id", pKey.toString());
			ArrayList<Integer> devices = new ArrayList<Integer>();
			HashMap<Integer, String> lsConf=new  HashMap<Integer, String>();
			HashMap<Integer, String> lsJournal=new  HashMap<Integer, String>();
			for (Text val : pValues) {
				try {
					JSONObject jsonObject=new JSONObject(val.toString());
					int conf=jsonObject.getInt("journal");
					String key=jsonObject.getString("key");
					int id=jsonObject.getInt("_id");
					devices.add(id);
					
					if(key.startsWith("conf"))
						lsConf.put(conf, "x");
					if(key.startsWith("journal"))
						lsJournal.put(conf, "x");
					
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
			BasicBSONObject update = new BasicBSONObject("$pushAll", new BasicBSONObject("pubs", devices)
			//.append("pubCount", devices.size())
			.append("ConfCount", lsConf.keySet())
			.append("JournalCount", lsJournal.keySet()));
			pContext.write(null, new MongoUpdateWritable(query, update, true, false));

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

	public MapReduceObject() throws UnknownHostException {
		setConf(new Configuration());


		if (MongoTool.isMapRedV1()) {
			MapredMongoConfigUtil.setInputFormat(getConf(), com.mongodb.hadoop.mapred.MongoInputFormat.class);
			MapredMongoConfigUtil.setOutputFormat(getConf(), com.mongodb.hadoop.mapred.MongoOutputFormat.class);
		} else {
			MongoConfigUtil.setInputFormat(getConf(), MongoInputFormat.class);
			MongoConfigUtil.setOutputFormat(getConf(), MongoOutputFormat.class);
		}

		MongoConfigUtil.setInputURI(getConf(),"mongodb://localhost:27017/nstar.publications.pubs");
		MongoConfigUtil.setOutputURI(getConf(),"mongodb://localhost:27017/nstar.publications.tauthors");

		MongoConfigUtil.setMapper(getConf(), MedlineKeywordMaper1.class);
		MongoConfigUtil.setReducer(getConf(), MedlineKeywordReduce1.class);

		MongoConfigUtil.setMapperOutputKey(getConf(), Text.class);
		MongoConfigUtil.setMapperOutputValue(getConf(), Text.class);

		MongoConfigUtil.setOutputKey(getConf(), IntWritable.class);
		MongoConfigUtil.setOutputValue(getConf(), BSONWritable.class);

	}

		public static void main(final String[] pArgs) throws Exception {
			System.exit(ToolRunner.run(new MapReduceObject(), null));
		}
}