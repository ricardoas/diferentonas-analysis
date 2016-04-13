package br.edu.ufcg.analytics.diferentonas.util;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianStemFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.AttributeFactory;

public class TextAnalyzer {

	public static String filter(String text) throws IOException {

		AttributeFactory factory = AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY;
		try (StandardTokenizer tokenizer = new StandardTokenizer(factory);
				TokenStream filter = new BrazilianStemFilter(
						new StopFilter(tokenizer, BrazilianAnalyzer.getDefaultStopSet()));) {

			tokenizer.setReader(new StringReader(text.toLowerCase().trim()));
			tokenizer.reset();

			CharTermAttribute attr = filter.addAttribute(CharTermAttribute.class);

			StringBuilder result = new StringBuilder();
			while (filter.incrementToken()) {
				String term = attr.toString();
				result.append(term + " ");
			}

			filter.close();
			return result.toString().trim();
		}

	}

}
