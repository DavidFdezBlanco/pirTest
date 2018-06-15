package pirTest;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

@SuppressWarnings({ "serial", "unused" })
public class UserVisitCountBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	private HashMap<Integer, Integer> userVisitCounts;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare( Map stormConf, TopologyContext context, OutputCollector collector) {
		userVisitCounts = new HashMap<Integer, Integer>();
		outputCollector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Integer userId = input.getIntegerByField("userId");
		Integer complex = input.getIntegerByField("complex");
		userVisitCounts.put(userId, userVisitCounts.getOrDefault(userId, 0) + 1);
		Utils.sleep(complex * 1000);
		outputCollector.ack(input);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}