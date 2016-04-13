package br.edu.ufcg.analytics.diferentonas.batch;


/**
 * @author Ricardo Araújo Santos - ricoaraujosantos@gmail.com
 *
 */
public enum SimilarityParameter {
	
	TX_OBJETO_CONVENIO("convenios", "nr_convenio", "tx_objeto_convenio", "similar_convenios_by_tx_objeto_convenio_");
	
	private String inputTableName;
	private String keyColumnName;
	private String valueColumnName;
	private String outputTableName;
	
	private SimilarityParameter(String inputTableName, String keyColumnName, String valueColumnName,
			String outputTableName) {
		this.inputTableName = inputTableName;
		this.keyColumnName = keyColumnName;
		this.valueColumnName = valueColumnName;
		this.outputTableName = outputTableName;
	}

	/**
	 * @return the inputTableName
	 */
	public String getInputTableName() {
		return inputTableName;
	}

	/**
	 * @return the keyColumnName
	 */
	public String getKeyColumnName() {
		return keyColumnName;
	}
	
	/**
	 * @return the valueColumnName
	 */
	public String getValueColumnName() {
		return valueColumnName;
	}

	/**
	 * @return the outputTableName
	 */
	public String getOutputTableName() {
		return outputTableName;
	}
}
