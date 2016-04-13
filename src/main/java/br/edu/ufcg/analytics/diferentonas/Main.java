package br.edu.ufcg.analytics.diferentonas;

import java.io.IOException;
import java.util.Arrays;

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
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ConfigurationException, IOException {
		Configuration config = new PropertiesConfiguration(args[0]);
		new SimilarityBatchJobSpark(config).run(Arrays.copyOfRange(args, 1, args.length));
	}
}
