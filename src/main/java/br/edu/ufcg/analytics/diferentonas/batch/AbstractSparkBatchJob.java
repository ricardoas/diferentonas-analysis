package br.edu.ufcg.analytics.diferentonas.batch;


import java.io.Serializable;
import java.util.Comparator;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 *
 */
public abstract class AbstractSparkBatchJob implements Serializable {

	private static final String KEYSPACE_PROP_NAME = "diferentonas.keyspace";

	protected class IDComparator implements Comparator<Integer>, Serializable {

		private static final long serialVersionUID = -3714006229470396098L;

		@Override
		public int compare(Integer a, Integer b) {
			return Integer.compare(a, b);
		}

	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -8740884438208244299L;
	private String keyspace;
	private transient JavaSparkContext sc;

	public AbstractSparkBatchJob(Configuration configuration) {
		this.sc = createJavaSparkContext(configuration);
		this.keyspace = configuration.getString(KEYSPACE_PROP_NAME);
	}

	private JavaSparkContext createJavaSparkContext(Configuration configuration) {
		SparkConf conf = new SparkConf();
		conf.setAppName(this.getClass().getSimpleName());
		return new JavaSparkContext(conf);
	}

	/**
	 * @return the sc
	 */
	public JavaSparkContext getJavaSparkContext() {
		return sc;
	}

	public SQLContext getSQLContext() {
		return new SQLContext(getJavaSparkContext());

	}

	public String getKeyspace() {
		return keyspace;
	}

	public abstract void run(String... args);
}
