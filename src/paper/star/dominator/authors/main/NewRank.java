/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paper.star.dominator.authors.main;

import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.FutureTask;

import org.jgrapht.DirectedGraph;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import generatordata.TestGraphData;
import paper.star.dominator.authors.algorithm.NStar;
import paper.star.dominator.authors.data.TestRun;
import paper.star.dominator.authors.dataset.ComparatorPageRank;

public class NewRank {
	//static ConnectionMongo connectionMongo=new ConnectionMongo("localhost","27017");
	public static void writeFile(TreeMap<Integer, Double> treeMap,String name)
			throws Exception {
		try {
			DB db;
			DBCollection dbresult;
			MongoClient mongo;
			mongo=new MongoClient("localhost",27017);
			db=mongo.getDB("nstar");
			dbresult=db.getCollection("result.full300");
			
			System.out.println("Start write a file...!");
			String path = "data/"+name+"--pageRank" + System.currentTimeMillis() + ".csv";
			FileWriter writer = new FileWriter(path);
			
			writer.append("ID");
		    writer.append(',');
		    writer.append("Value");
		    writer.append('\n');
			for (Entry<Integer, Double> entry : treeMap.entrySet()) {
				writer.append(""+entry.getKey());
			    writer.append(',');
			    writer.append(""+entry.getValue());
			    writer.append('\n');
			    dbresult.insert(new BasicDBObject().append("_id", entry.getKey())
			    		.append("value", entry.getValue()));
			}
			   writer.flush();
			    writer.close();
			System.out.println("...Write file is successfully!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		
		TestGraphData bg = new TestGraphData();
		TestRun tr = new TestRun();
		
		//DirectedGraph<Integer, Integer> graph = bg.initData();
		DirectedGraph<Integer, Integer> graph = tr.chay();
				
		Double dampingFactor1 = 0.45d;
                Double dampingFactor2 = 0.45d;
		
		NStar<Integer, Integer> pr = new NStar<>(graph,
				dampingFactor1, dampingFactor2);
//		ForStart<Integer, Integer> pr = new ForStart<>(graph,
//				dampingFactor1, dampingFactor2);
		
		FutureTask<HashMap<Integer, Double> >ft=new FutureTask<HashMap<Integer,Double>>(pr);
		
		new Thread(ft).start();
		while(!ft.isDone());
		HashMap<Integer, Double> computePageRank=ft.get();
		//HashMap<String, Double> computePageRank = pr.call();
		 //System.out.println(""+computePageRank.toString());
                 
		ComparatorPageRank comparatorPageRank = new ComparatorPageRank(
				computePageRank);
		TreeMap<Integer, Double> treeMap = new TreeMap<Integer, Double>(
				comparatorPageRank);
		treeMap.putAll(computePageRank);
		
		writeFile(treeMap,"pubs");
		//connectionMongo.addRanking(treeMap);
		System.out.println("Ok");
	}
}
