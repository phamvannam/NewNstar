/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package generatordata;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;

/**
 *
 * @author now
 */
public class TestGraphData {
	//    public static void main(String[] args) {
	//        DirectedGraph<Integer,Integer> graph=initData();
	//    }
	public  DirectedGraph<Integer, Integer> initData() {

		DirectedGraph<Integer,Integer> graph=new DefaultDirectedGraph<>(Integer.class);

		int p1 = 1;
		int p2 = 2;
		int p3 =3;
		int p4 =4;

		int a1=11;
		int a2=22;
		int a3=33;
		int a5=33;
		int a4=33;
		int a6=33;

		int c1=111;
		int c2=222;

		graph.addVertex(a1);
		graph.addVertex(a2);
		graph.addVertex(a3);


		graph.addVertex(c1);
		graph.addVertex(c2);

		graph.addVertex(p1);
		graph.addVertex(p2);
		graph.addVertex(p3);
		//graph.addVertex(p4);

		//              
		graph.addEdge(p1, a1,1111);
		graph.addEdge(p1, a2,2222);
		graph.addEdge(p1, a3,3333);
		graph.addEdge(p2, a1,4444);
		//graph.addEdge(p2, a3,5555);
		graph.addEdge(p3, a2,6666);
		//graph.addEdge(p3, a3,7666);
		//                graph.addEdge(p4, a3,8666);
		//                graph.addEdge(p4, a2,9666);
		//                graph.addEdge(p4, a1,6766);

		graph.addEdge(p1, c1,p1);
		graph.addEdge(p2, c2,p2);
		graph.addEdge(p3, c2,p3);
		//graph.addEdge(p4, c2,p4);
		//                
		//                  m
		//                graph.addEdge(p1, a1);
		//                graph.addEdge(p1, a3);
		//                graph.addEdge(p1, a2);
		//                graph.addEdge(p2, a1);
		//                graph.addEdge(p2, a2);
		//                graph.addEdge(p3, a1);
		//                
		//                graph.addEdge(p1, c1);
		//                graph.addEdge(p2, c1);
		//                graph.addEdge(p3, c2);

		//                Set<Integer> psss=graph.edgesOf(c1);
		//                for (Integer pss : psss) {
		//                System.out.println("source: "+graph.getEdgeSource(pss)+"--target: "+graph.getEdgeTarget(pss));
		//                }
		//                        
		//                System.out.println("---"+psss.toString());
		return graph;

	}
	private static void thuattoan(){

	}
}
