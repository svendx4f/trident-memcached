package trident.memcached;

import java.io.Serializable;
import java.util.List;

public interface IStateSingleKeyBuilder extends Serializable{

	/**
	 * @return a single field primary key from this compound key
	 */
	public String buildSingleKey(List<Object> key) ;
	
}
