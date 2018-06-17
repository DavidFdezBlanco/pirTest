package PirLoad;

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
import java.io.PrintWriter;
import java.io.Writer;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;

@SuppressWarnings({ "serial", "unused" })
public class UserVisitCountBolt extends BaseRichBolt {
	private OutputCollector outputCollector;
	private HashMap<Integer, Integer> userVisitCounts;
	private long stopTime ;
	private long waitingTime;
	private FileWriter printWriter;
	private File file;
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare( Map stormConf, TopologyContext context, OutputCollector collector) {
		userVisitCounts = new HashMap<Integer, Integer>();
		outputCollector = collector;
		file = new File ("waiting.csv");
	}

	@Override
	public void execute(Tuple input) {
		Integer id = input.getIntegerByField("id");
		Integer userId = input.getIntegerByField("userId");
		Integer complex = input.getIntegerByField("complex");
		long start = input.getLongByField("start");
		userVisitCounts.put(userId, userVisitCounts.getOrDefault(userId, 0) + 1);
		Utils.sleep(complex * 400);
		outputCollector.ack(input);
		stopTime = System.currentTimeMillis();
		waitingTime = stopTime - start - (complex*400);
		System.out.println("Task " + id + " Complexity " + complex + " Waiting Time " + waitingTime + " ms.");
		
		try {
			printWriter = new FileWriter (file, true);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			printWriter.write (id+";"+waitingTime+";\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			printWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}