/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paper.star.dominator.authors.main;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author five
 */
public class Test {
	private static boolean key=false;
    public static void main(String[] args) {
    	ExecutorService executor;
		executor = Executors.newFixedThreadPool(20);
		HashMap<Integer, Integer> a=new HashMap<>();
		HashMap<Integer, Integer> b=new HashMap<>();
		for (int i = 0; i < 10000; i++) {
			a.put(i, i+10);
		}
		for (int i = 0; i < 10000; i++) {
			final int x=i;
			Runnable worker = new Runnable() {
				public void run() 
				{
					b.put(x, 10-x);
					System.out.println(x);
					if(a.get(x)-b.get(x)>0){
						setKey(true);
					}
				}
			};
			executor.execute(worker);
		}
		executor.shutdown();
        while (!executor.isTerminated()) {
        	//System.out.println("xxx");
        }
       
	}
	public static boolean isKey() {
		return key;
	}
	public static void setKey(boolean key) {
		Test.key = key;
	}
}
