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

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;

import org.apache.hadoop.conf.Configuration;
public class MapReduceSearch extends MongoTool{


	public static class MedlineKeywordMaper1 extends Mapper <Object, BSONObject,Text, Text>  
	implements org.apache.hadoop.mapred.Mapper<Object, BSONWritable, Text, IntWritable> {
		@Override
		public void map(Object key, BSONObject val,
				Context context)

						throws IOException, InterruptedException {
			String _id=(String)val.get("_id");
			Text txout=new Text();
			if (val.containsField("pubs")) {
				@SuppressWarnings("unchecked")
				ArrayList<Integer> pubList=(ArrayList<Integer>) val.get("pubs");
				for (int i = 0; i < pubList.size(); i++) {
					int value= pubList.get(i);
					txout.set(value+"");	
					if (txout.getLength()>0) {
						context.write( txout,new Text(_id+""));
					}	
				}
			}
//			if (val.containsField("authors")) {
//				ArrayList<String> autList=(ArrayList<String>) val.get("authors");
//				for (int i = 0; i < autList.size(); i++) {
//					String value= autList.get(i).toString();
//					txout.set(value);	
//					if (txout.getLength()>0) {
//						context.write( txout,new Text(_id));
//					}	
//				}
//			}
//			if (val.containsField("journal")&&val.get("key").toString().startsWith("journal")) {
//					String value= val.get("journal").toString();
//					txout.set(value);	
//					if (txout.getLength()>0) {
//						context.write( txout,new Text(_id+"-"+val.get("year")));
//					}	
//			}

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
			ArrayList<String> devices = new ArrayList<String>();
			for (Text val : pValues) {
//				String s=val.toString();
//				String id=s.substring(0,s.indexOf("-"));
//				int year=Integer.parseInt(s.substring(s.indexOf("-")+1));
//				if(year<y)
//					y=year;
				devices.add(val.toString());
			}
			
				BasicBSONObject update = new BasicBSONObject("$pushAll", new BasicBSONObject("pubs", devices));
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
	private static String keyword="";

	public static String getKeyword() {
		return keyword;
	}

	public static void setKeyword(String keyword) {
		MapReduceSearch.keyword = keyword;
	}

	public MapReduceSearch() throws UnknownHostException {
		setConf(new Configuration());


		if (MongoTool.isMapRedV1()) {
			MapredMongoConfigUtil.setInputFormat(getConf(), com.mongodb.hadoop.mapred.MongoInputFormat.class);
			MapredMongoConfigUtil.setOutputFormat(getConf(), com.mongodb.hadoop.mapred.MongoOutputFormat.class);
		} else {
			MongoConfigUtil.setInputFormat(getConf(), MongoInputFormat.class);
			MongoConfigUtil.setOutputFormat(getConf(), MongoOutputFormat.class);
		}

		MongoConfigUtil.setInputURI(getConf(),"mongodb://localhost:27017/nstar.publications.tauthors");
		MongoConfigUtil.setOutputURI(getConf(),"mongodb://localhost:27017/nstar.publications.tpubs");

		MongoConfigUtil.setMapper(getConf(), MedlineKeywordMaper1.class);
		MongoConfigUtil.setReducer(getConf(), MedlineKeywordReduce1.class);

		MongoConfigUtil.setMapperOutputKey(getConf(), Text.class);
		MongoConfigUtil.setMapperOutputValue(getConf(), Text.class);

		MongoConfigUtil.setOutputKey(getConf(), IntWritable.class);
		MongoConfigUtil.setOutputValue(getConf(), BSONWritable.class);

		
	}

	public static void main(final String[] pArgs) throws Exception {
		System.exit(ToolRunner.run(new MapReduceSearch(), null));
	}
}