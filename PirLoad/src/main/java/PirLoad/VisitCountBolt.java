package PirLoad;

import java.util.Iterator;
import java.util.List;
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
	private List<Integer> numCounterTasks;
	private Integer coord;
	private Iterator<Integer> myListIterator; 
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare( Map stormConf, TopologyContext context, OutputCollector collector) {
		totalVisitCount = 0;
		outputCollector = collector;
		this.numCounterTasks = context.getComponentTasks("user-visit-counts");
		System.out.println("The Tasks Ids : " + numCounterTasks);
		myListIterator = numCounterTasks.iterator(); 
	}
	
	@Override
	public void execute(Tuple input) {
		Integer id = input.getIntegerByField("id");
		String url = input.getStringByField("url");
		Integer userId = input.getIntegerByField("userId");
		Integer complex = input.getIntegerByField("complex");
		long start = input.getLongByField("start");
		if (myListIterator.hasNext()) {
		    coord = myListIterator.next(); 
		}else {
			myListIterator = numCounterTasks.iterator();
			coord = myListIterator.next();
		}
		totalVisitCount += 1;
		System.out.print("Id of the processor :" + coord + ". ");
		outputCollector.emitDirect(coord, input,new Values(id, url, userId, complex, start));
		outputCollector.ack(input);
		System.out.println(url + " asked by : "+ userId + ". Complexity = " + complex);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "url", "userId", "complex", "start"));
	}
	
	public Integer getWordCountIndex(String word) {
        word = word.trim().toUpperCase();
        if(word.isEmpty())
            return 0;
        else
            return 0;
    }
}