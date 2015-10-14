/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paper.star.dominator.authors.algorithm;

import java.io.FileWriter;
/**
 *
 * @author now
 */
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jgrapht.DirectedGraph;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import paper.star.dominator.authors.dataset.ComparatorPageRank;

public class NStar<T, E> implements Callable<HashMap<Integer, Double>> {

	private HashMap<Integer, Double> lastRanking, nextRanking, AuRank, ConRank;

	private DirectedGraph<Integer, Integer> graph;
	private Double dampingFactor1, dampingFactor2;//, oldStringalue, newStringalue;
	//private static final long currentTime = System.currentTimeMillis();
	//private ConnectionMongo connectionMongo;
	private Integer max =4000000;
	public NStar(DirectedGraph<Integer, Integer> graph,
			final Double dampingFactor1, final Double dampingFactor2) {
		lastRanking = new HashMap<Integer, Double>();
		nextRanking = new HashMap<Integer, Double>();
		AuRank = new HashMap<Integer, Double>();
		ConRank = new HashMap<Integer, Double>();
		this.graph = graph;
		this.dampingFactor1 = dampingFactor1;
		this.dampingFactor2 = dampingFactor2;
		//connectionMongo = new ConnectionMongo("localhost", "27017");
	}
	private boolean k = false;
	public HashMap<Integer, Double> computePageRank() {

		// Initialize "last" ranking with initial values.
		for (Integer v : graph.vertexSet()) {
			if (v.hashCode() < max) {
				lastRanking.put(v, 100.0d);
			}
		}
		System.out.println("xxx" + lastRanking.get(1));
		double dampingFactorComplement = (1.0 - (dampingFactor1 + dampingFactor2));

		System.out.println(lastRanking.keySet().size());
		//System.out.println(graph.toString());

		int interval = 1;
		do {
			k = false;

			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Calendar cal = Calendar.getInstance();
			System.err.println("================================================");
			System.out.println("Start: " + dateFormat.format(cal.getTime()));
			nextRanking = new HashMap<Integer, Double>();

			for (Integer vx : lastRanking.keySet()) {
				//System.out.println("ix"+interval);
				//if (v.hashCode() < 4000000)
				final Integer v=vx;
//				Runnable worker = new Runnable() {
//					public void run() 
//					{

						ArrayList<Integer> egdes = new ArrayList<>((Set<Integer>) graph.edgesOf(v));
						Integer conf = getConf(egdes);
						ArrayList<Integer> lsAu = getAuPub(v);
						ArrayList<Integer> lsPubOfConf = new ArrayList<>((Set<Integer>) graph.edgesOf(conf));

						double totalNextRank = 0.0d;
						double totalRankingAu = 0.0d;
						double totalRankingConf = 0.0d;
						// 
						for (int i = 0; i < lsAu.size(); i++) {
							double rankAu = rankingAuthorNew(lsAu.get(i), lsAu.size(), i, v);
							totalRankingAu += rankAu;

						}

						double rankingconf = 0.0d;
						rankingconf = rankingConf(lsPubOfConf,conf);
						
						totalRankingConf = rankingconf / lsPubOfConf.size();

						totalRankingAu = (dampingFactor1 * totalRankingAu);
						totalRankingConf = (dampingFactor2 * totalRankingConf);

						totalNextRank = totalRankingAu
								+ totalRankingConf
								+ dampingFactorComplement * (100.0d);

						nextRanking.put(v, totalNextRank);
						if (Math.abs(nextRanking.get(v) - lastRanking.get(v)) > (10E-3)) {
							k = true;
						}
//					}
//				};
//				executor.execute(worker);
			}
//			executor.shutdown();
//            while (!executor.isTerminated()) {
//
//            }
            
//            executor = Executors.newFixedThreadPool(100);
//            for (Integer v : lastRanking.keySet()) {
//            		Runnable worker = new Runnable() {
//            		public void run() 
//					{
//            			if (Math.abs(nextRanking.get(v) - lastRanking.get(v)) > (10E-3)) {
//            				k = true;
//            			}
//            	}};
//				executor.execute(worker);
//            }
//            executor.shutdown();
//            while (!executor.isTerminated()) {
//
//            }
			//);
			System.out.println("--"+interval+"---------");
			showRank();
			lastRanking = nextRanking;

			//System.out.println("ls" + lastRanking.toString());
			interval++;
		} while (k);
		writels(AuRank,"AU");
		writels(ConRank,"Journal");
		return lastRanking;
	}

	private void writels(HashMap<Integer, Double> auR,String name) {
		ComparatorPageRank comparatorPageRank = new ComparatorPageRank(
				(HashMap<java.lang.Integer, Double>) auR);
		TreeMap<java.lang.Integer, Double> treeMap = new TreeMap<java.lang.Integer, Double>(
				comparatorPageRank);
		treeMap.putAll((Map<? extends java.lang.Integer, ? extends Double>) auR);
		try {
			writeFile((TreeMap<Integer, Double>) treeMap, name);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private double rankingConf(ArrayList<Integer> lsPubOfConf,Integer conf) {
		double result = 0.0d;
		for (int i = 0; i < lsPubOfConf.size(); i++) {
			result += lastRanking.get(lsPubOfConf.get(i));
		}
		if (ConRank.containsKey(conf)) {
			ConRank.remove(conf);
			ConRank.put(conf, result);
		} else {
			//System.out.println(conf+"------"+result);
			ConRank.put(conf, result);
		}
		return result;
	}

	private double rankingAuthorNew(Integer au, int index, int size, Integer pu) {
		ArrayList<Integer> lsPubAu = getPubAu(au);
		//System.out.println("--------------------");
		//double otherval = 0.0d;
		double totalRankPub = 0.0d;
		HashMap<Integer, Double> value = new HashMap<>();
		for (int i = 0; i < lsPubAu.size(); i++) {
			Integer puVal = lsPubAu.get(i);
			ArrayList<Integer> lsAuInPub = getAuPub(puVal);
			int si = lsAuInPub.size();
			int idx = lsAuInPub.indexOf(au);
			double tmp = 1.0d;
			if (si > 1) {
				tmp = (double) ((si) * 1.0d - idx * 1.0d) / (1.0d * ((si * (1 + si)) / 2.0d));
			}
			//System.out.println(au+"=tmp="+tmp);
			value.put(puVal, tmp);
			try{
				totalRankPub += (tmp * lastRanking.get(puVal));
			}catch(Exception e){
				System.out.println("pu: "+puVal);
				System.out.println("a"+au);;
			}

			//  otherval += tmp;
		}
		//otherval = (value.get(pu)) / (otherval);
		if (AuRank.containsKey(au)) {
			AuRank.remove(au);
			AuRank.put(au, totalRankPub);
		} else {
			AuRank.put(au, totalRankPub);
		}
		totalRankPub = totalRankPub /(lsPubAu.size()*1.0d);

		//System.out.println("pu: "+pu+"--rank"+au+"--"+totalRankPub);
		return (double) totalRankPub;
	}



	private Integer getConf(ArrayList<Integer> v) {
		Integer idx = graph.getEdgeTarget(v.get(v.size() - 1));
		return idx;

	}

	private ArrayList<Integer> getAuPub(Integer get) {
		ArrayList<Integer> v = new ArrayList<Integer>((Set<Integer>) graph.edgesOf(get));
		ArrayList<Integer> result = new ArrayList<>();
		for (int i = 0; i < v.size() - 1; i++) {
			Integer v1 = graph.getEdgeTarget(v.get(i));
			result.add(v1);
		}
		return result;
	}

	private ArrayList<Integer> getPubAu(Integer get) {
		ArrayList<Integer> v = new ArrayList<Integer>((Set<Integer>) graph.edgesOf(get));
		ArrayList<Integer> result = new ArrayList<>();
		for (int i = 0; i < v.size(); i++) {
			result.add(graph.getEdgeSource(v.get(i)));
		}
		return result;
	}

	private void showRank() {
		double sum1 = 0.0d, sum2 = 0.0d, sum3 = 0.0d;
		for (Map.Entry<Integer, Double> entrySet : lastRanking.entrySet()) {
			Double value = entrySet.getValue();
			sum1 += value;
		}
		System.err.println("rp:" + sum1 + "-- :");
		//System.err.println("rp:" + sum1 + "-- :" + lastRanking.toString());
		for (Map.Entry<Integer, Double> entrySet : AuRank.entrySet()) {
			//Integer key = entrySet.getKey();
			Double value = entrySet.getValue();
			sum2 += value;
		}
		System.err.println("ra:" + sum2 + "-- :");
		//System.err.println("ra:" + sum2 + "-- :" + AuRank.toString());
		for (Map.Entry<Integer, Double> entrySet : ConRank.entrySet()) {
			// Integer key = entrySet.getKey();
			Double value = entrySet.getValue();
			sum3 += value;
		}
		System.err.println("rc:" + sum3 + "-- :" );
		//System.err.println("rc:" + sum3 + "-- :" + ConRank.toString());
	}
	public void writeFile(TreeMap<Integer, Double> treeMap,String name)
			throws Exception {
		try {
			DB db;
			DBCollection dbresult;
			MongoClient mongo;
			mongo=new MongoClient("localhost",27017);
			db=mongo.getDB("nstar");
			dbresult=db.getCollection("result.full300"+name);
			
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
	@Override
	public HashMap<Integer, Double> call() throws Exception {
		return computePageRank();
	}
}
