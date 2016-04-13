package br.edu.ufcg.analytics.diferentonas;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import br.edu.ufcg.analytics.diferentonas.batch.SimilarityBatchJobSpark;

/**
 * Entry point.
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 */
public class Main {
	
	/**
	 * 
	 * @param args
	 * @throws ConfigurationException
	 */
	public static void main(String[] args) throws ConfigurationException {
		Configuration config = new PropertiesConfiguration(args.length > 0? args[0]: "diferentonas.properties");
		new SimilarityBatchJobSpark(config).run();
	}

}
