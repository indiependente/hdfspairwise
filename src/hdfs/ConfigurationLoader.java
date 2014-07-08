package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ConfigurationLoader {
	//dfs.block.size for ps
	public static final String BLOCK_SIZE = "dfs.blocksize";
	
	//public static final String HADOOP_HOME = "/";
	
	//For ps
	public static final String HADOOP_HOME = "/usr/local/hadoop/etc/hadoop/";
	
	//ps hdfs-site.xml
	
	
	private static final String[] configFilesToLoad = { "core-site.xml", "hdfs-site.xml" };
	
	private Configuration conf;
	
	private ConfigurationLoader()
	{
		
		conf = new Configuration();
		for (String s : configFilesToLoad)
			conf.addResource(new Path(HADOOP_HOME + s));
		
	}
	
	public Configuration getConfiguration()
	{
		return conf;
	}
	
	public String getValue(String key)
	{
		return conf.get(key);
	}
	
	public int getIntValue(String key)
	{
		return Integer.parseInt(getValue(key));
	}
	
	
	private static ConfigurationLoader instance = null;
	public static ConfigurationLoader getInstance()
	{
		if (instance == null)
			instance = new ConfigurationLoader();
		return instance;
	}

	
}
