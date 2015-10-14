package paper.star.dominator.authors.dataset;

import java.util.Comparator;
import java.util.HashMap;

/**
 * @author sky
 * @param <T>
 */
public class ComparatorPageRank implements Comparator<Integer> {

	private HashMap<Integer, Double> lastRanking;

	public ComparatorPageRank(HashMap<Integer, Double> lastRanking) {
		this.lastRanking = lastRanking;
	}

	@Override
	public int compare(Integer o1, Integer o2) {
		Double val1 = lastRanking.get(o1);
		Double val2 = lastRanking.get(o2);
		if (val1.compareTo(val2) <= 0)
			return 1;
		else if (val1.compareTo(val2) > 0)
			return -1;
		return 0;
	}

}
