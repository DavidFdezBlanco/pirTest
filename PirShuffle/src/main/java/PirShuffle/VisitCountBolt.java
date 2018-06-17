package PirShuffle;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings("serial")
public class VisitCountBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	private Integer totalVisitCount;
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare( Map stormConf, TopologyContext context, OutputCollector collector) {
		totalVisitCount = 0;
		outputCollector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		Integer id = input.getIntegerByField("id");
		String url = input.getStringByField("url");
		Integer userId = input.getIntegerByField("userId");
		Integer complex = input.getIntegerByField("complex");
		long start = input.getLongByField("start");
		
		totalVisitCount += 1;
		outputCollector.emit(input,new Values(id, url, userId, complex, start));
		outputCollector.ack(input);
		System.out.println(url + " asked by : "+ userId + ". Complexity = " + complex);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "url", "userId", "complex", "start"));
	}

}