package trident.memcached;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import java.net.InetSocketAddress;
import java.util.Arrays;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;
import trident.memcached.MemcachedState.Options;


public class Test {
    private static final MemCacheDaemon<LocalCacheElement> daemon =
            new MemCacheDaemon<LocalCacheElement>();
    
    private static void startLocalMemcacheInstance(int port) {
        System.out.println("Starting local memcache");
        CacheStorage<Key, LocalCacheElement> storage =
                ConcurrentLinkedHashMap.create(
                        ConcurrentLinkedHashMap.EvictionPolicy.FIFO, 100, 1024*500);
        daemon.setCache(new CacheImpl(storage));
        daemon.setAddr(new InetSocketAddress("localhost", port));
        daemon.start();
    }

     /**
      * splits a space-separated sentence into words
     *
     */
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for(String word: sentence.split(" ")) {
                collector.emit(new Values(word));                
            }
        }
    }
     
     /**
      * Emit the first letter and the lenght of each word longer than 2 letters
     *
     */
    public static class Initial extends BaseFunction {
    	@Override
    	 public void execute(TridentTuple tuple, TridentCollector collector) {
    		 String word = tuple.getString(0);
			 if (word.length() > 2) {
				 collector.emit(new Values(word.substring(0, 1), word.length()));                
			 }
    	 }
     }
    
    /**
     * expects a comma-separated queries like "t 3, a 6" and emit tuples like ('t', 3), ('a', 6)
     *
     */
    public static class ParseCli extends BaseFunction {
    	@Override
    	public void execute(TridentTuple tuple, TridentCollector collector) {
    		String queries = tuple.getString(0);
    		for(String query: queries.split(",")) {
    			String[] splitted = query.trim().split(" ");
    			collector.emit(new Values(splitted[0].trim(), splitted[1].trim()));
    		}
    		
    	}
    }
    
    public static StormTopology buildTopology(LocalDRPC drpc,  int memcachedPort) {
    	
    	////////////////
    	// infinite source of words
    	
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"),
                new Values("to be or not to be the person"));
        spout.setCycle(true);
        
        
        TridentTopology topology = new TridentTopology();        
        Stream words =              
        		topology.newStream("spout1", spout)
                .each(new Fields("sentence"), new Split(), new Fields("word"));
 
        
    	////////////////
        // counting words + DRCP access to result
       
        
        Options wordsOpt = new Options() ; 
        wordsOpt.keyBuilder = new ConcatKeyBuilder("WORD_COUNT");
        StateFactory memcachedWords = MemcachedState.nonTransactional(Arrays.asList(new InetSocketAddress("localhost", memcachedPort)), wordsOpt);
        
        TridentState wordCounts =
        		words
                .groupBy(new Fields("word"))
                .persistentAggregate(memcachedWords, new Count(), new Fields("count"))    
                .parallelismHint(6);        
                
        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("wordCount"))
                .each(new Fields("wordCount"), new FilterNull())
                .aggregate(new Fields("wordCount"), new Sum(), new Fields("sum"))
                ;
        
    	////////////////
        // counting word having the same length and first letter + real-time DRCP access
        
        Options letterOpt = new Options();
        letterOpt.keyBuilder = new ConcatKeyBuilder("INITAL_COUNT");
        StateFactory memcachedInitials = MemcachedState.nonTransactional(Arrays.asList(new InetSocketAddress("localhost", memcachedPort)), letterOpt);

        TridentState initialCounts =
        		words
        		.each(new Fields("word"), new Initial(), new Fields("firstLetter", "length"))
        		.groupBy(new Fields("firstLetter", "length"))
        		.persistentAggregate(memcachedInitials, new Count(), new Fields("initCount"))         
        		.parallelismHint(6);
        
        topology.newDRPCStream("1rstLetter", drpc)
        		.each(new Fields("args"), new ParseCli(), new Fields("firstLetter", "length"))
        		.groupBy(new Fields("firstLetter", "length"))
                .stateQuery(initialCounts, new Fields("firstLetter", "length"), new MapGet(), new Fields("initCount"))
                .project(new Fields("firstLetter", "length", "initCount"))
                ;
        
        return topology.build();
    }
    
    
    
    public static void main(String[] args) {
        int PORT = 10001;
        startLocalMemcacheInstance(PORT);
        
        LocalDRPC wordsDrpc = new LocalDRPC();
        StormTopology topology = buildTopology(wordsDrpc,  PORT);
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("tester", conf, topology);
        
        for(int i=0; i<100; i++) {
            System.out.println("DRPC: number of instances of 'cat', 'the', 'man' or 'four' counted so far: " + wordsDrpc.execute("words", "cat the man four"));
            System.out.println("DRPC: number of instances of 3-letter word starting with 't' or 6-letter word starting with 'a': " + wordsDrpc.execute("1rstLetter", "t 3, a 6"));
            Utils.sleep(1000);
        }
        
   }
}
