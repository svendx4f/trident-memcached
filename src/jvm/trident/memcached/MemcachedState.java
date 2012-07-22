package trident.memcached;

import backtype.storm.tuple.Values;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import net.spy.memcached.CachedData;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.Transcoder;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;

public class MemcachedState<T> implements IBackingMap<T> {
    public static class Options {
        int localCacheSize = 1000;
        String globalKey = "$GLOBAL$";
    }
    
    public StateFactory opaque(List<InetSocketAddress> servers, Serializer<OpaqueValue> serializer) {
        return opaque(servers, serializer, new Options());
    }

    public StateFactory opaque(List<InetSocketAddress> servers, Serializer<OpaqueValue> serializer, Options opts) {
        return new Factory(servers, StateType.OPAQUE, serializer, opts);
    }
    
    public StateFactory transactional(List<InetSocketAddress> servers, Serializer<OpaqueValue> serializer) {
        return transactional(servers, serializer, new Options());
        
    }
    
    public StateFactory transactional(List<InetSocketAddress> servers, Serializer<OpaqueValue> serializer, Options opts) {
        return new Factory(servers, StateType.TRANSACTIONAL, serializer, opts);
    } 
    
    public StateFactory nonTransactional(List<InetSocketAddress> servers, Serializer<OpaqueValue> serializer) {
        return nonTransactional(servers, serializer, new Options());
        
    }
    
    public StateFactory nonTransactional(List<InetSocketAddress> servers, Serializer<OpaqueValue> serializer, Options opts) {
        return new Factory(servers, StateType.NON_TRANSACTIONAL, serializer, opts);       
    }      
    
    protected static class Factory implements StateFactory {
        StateType _type;
        List<InetSocketAddress> _servers;
        Serializer _ser;
        Options _opts;
        
        public Factory(List<InetSocketAddress> servers, StateType type, Serializer ser, Options options) {
            _type = type;
            _servers = servers;
            _opts = options;
        }
        
        @Override
        public State makeState(Map conf, int partitionIndex, int numPartitions) {
            ConnectionFactoryBuilder builder =
                    new ConnectionFactoryBuilder()
                        .setTranscoder(new Transcoder<Object>() {

                @Override
                public boolean asyncDecode(CachedData cd) {
                    return false;
                }

                @Override
                public CachedData encode(Object t) {
                    return new CachedData(0, _ser.serialize(t), CachedData.MAX_SIZE);
                }

                @Override
                public Object decode(CachedData data) {
                    return _ser.deserialize(data.getData());
                }

                @Override
                public int getMaxSize() {
                    return CachedData.MAX_SIZE;
                }
            });
            MemcachedState s;
            try {
                s = new MemcachedState(new MemcachedClient(builder.build(), _servers));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            CachedMap c = new CachedMap(s, _opts.localCacheSize);
            MapState ms;
            if(_type == StateType.NON_TRANSACTIONAL) {
                ms = NonTransactionalMap.build(c);
            } else if(_type==StateType.OPAQUE) {
                ms = OpaqueMap.build(c);
            } else if(_type==StateType.TRANSACTIONAL){
                ms = TransactionalMap.build(c);
            } else {
                throw new RuntimeException("Unknown state type: " + _type);
            }
            return new SnapshottableMap(ms, new Values(_opts.globalKey));
        }   
    }
    
    MemcachedClient _client;
    
    public MemcachedState(MemcachedClient client) {
        _client = client;
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<String> singleKeys = new ArrayList();
        for(List<Object> key: keys) {
            singleKeys.add(toSingleKey(key));
        }
        Map<String, Object> result = _client.getBulk(singleKeys);
        List<T> ret = new ArrayList(singleKeys.size());
        for(String k: singleKeys) {
            ret.add((T)result.get(k));
        }
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        List<OperationFuture<Boolean>> futures = new ArrayList(keys.size());
        for(int i=0; i<keys.size(); i++) {
            String key = toSingleKey(keys.get(i));
            T val = vals.get(i);
            futures.add(_client.set(key, 0, val));
        }
        for(OperationFuture<Boolean> future: futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private String toSingleKey(List<Object> key) {
        if(key.size()!=1) {
            throw new RuntimeException("Memcached state does not support compound keys");
        }
        return (String) key.get(0);
    }
    
}