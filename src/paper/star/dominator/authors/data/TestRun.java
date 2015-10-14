package paper.star.dominator.authors.data;

import java.net.UnknownHostException;

import org.apache.hadoop.util.ToolRunner;
import org.jgrapht.DirectedGraph;

public class TestRun {
	private int i;
	public TestRun() {
	}
	public DirectedGraph<Integer,Integer> chay() throws UnknownHostException, Exception {
		BuidGrapFromMongo mapReduceSearch=new BuidGrapFromMongo();
		i = ToolRunner.run(mapReduceSearch, null);
		return BuidGrapFromMongo.getGraphx();
	}
}
