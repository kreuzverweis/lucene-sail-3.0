/*
 * Copyright Aduna, DFKI and L3S (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene;

import static org.openrdf.model.vocabulary.RDF.TYPE;
import static org.openrdf.sail.lucene.LuceneSailSchema.FROM;
import static org.openrdf.sail.lucene.LuceneSailSchema.GEOLAT;
import static org.openrdf.sail.lucene.LuceneSailSchema.GEOLONG;
import static org.openrdf.sail.lucene.LuceneSailSchema.GEOTOLERANCE;
import static org.openrdf.sail.lucene.LuceneSailSchema.LUCENE_QUERY;
import static org.openrdf.sail.lucene.LuceneSailSchema.MATCHES;
import static org.openrdf.sail.lucene.LuceneSailSchema.PROPERTY;
import static org.openrdf.sail.lucene.LuceneSailSchema.QUERY;
import static org.openrdf.sail.lucene.LuceneSailSchema.SCORE;
import static org.openrdf.sail.lucene.LuceneSailSchema.SNIPPET;
import static org.openrdf.sail.lucene.LuceneSailSchema.TO;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.store.StoreException;

/**
 * A QueryInterpreter creates a set of QuerySpecs based on Lucene-related
 * StatementPatterns that it finds in a TupleExpr.
 * 
 * <p>
 * QuerySpecs will only be created when the set of StatementPatterns is complete
 * (i.e. contains at least a matches and a query statement connected properly)
 * and correct (query pattern has a literal object, matches a resource subject,
 * etc.).
 */
public class QuerySpecBuilder {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	private final boolean incompleteQueryFails;

	/**
	 * Initialize a new QuerySpecBuilder
	 * 
	 * @param incompleteQueryFails
	 *        see {@link LuceneSail#isIncompleteQueryFails()}
	 */
	public QuerySpecBuilder(boolean incompleteQueryFails) {
		this.incompleteQueryFails = incompleteQueryFails;
	}

	/**
	 * Returns a set of QuerySpecs embodying all necessary information to perform
	 * the Lucene query embedded in a TupleExpr.
	 */
	public Set<QuerySpec> process(TupleExpr tupleExpr, BindingSet bindings)
		throws StoreException
	{
		HashSet<QuerySpec> result = new HashSet<QuerySpec>();

		// find Lucene-related StatementPatterns
		PatternFilter filter = new PatternFilter();
		tupleExpr.visit(filter);

		// loop over all matches statements
		for (StatementPattern matchesPattern : filter.matchesPatterns) {
			// the subject of the matches statements should be a variable or a
			// Resource
			Var subjectVar = matchesPattern.getSubjectVar();
			Value subjectValue = subjectVar.hasValue() ? subjectVar.getValue()
					: bindings.getValue(subjectVar.getName());

			if (subjectValue != null && !(subjectValue instanceof Resource)) {
				failOrWarn(MATCHES + " properties should have Resource subjects: " + subjectVar.getValue());
				continue;
			}

			Resource subject = (Resource)subjectValue;

			// the matches var should have no value
			Var matchesVar = matchesPattern.getObjectVar();
			if (matchesVar.hasValue()) {
				failOrWarn(MATCHES + " properties should have variable objects: " + matchesVar.getValue());
				continue;
			}

			// find the relevant outgoing patterns
			StatementPattern typePattern, queryPattern, propertyPattern, scorePattern, snippetPattern;
			// GEO extensions - kreuzverweis
			StatementPattern fromPattern, toPattern, geoLatPattern, geoLongPattern, geoTolerancePattern;
			// end GEO extensions - kreuzverweis

			try {
				typePattern = getPattern(matchesVar, filter.typePatterns);
				queryPattern = getPattern(matchesVar, filter.queryPatterns);
				propertyPattern = getPattern(matchesVar, filter.propertyPatterns);
				scorePattern = getPattern(matchesVar, filter.scorePatterns);
				snippetPattern = getPattern(matchesVar, filter.snippetPatterns);
				// GEO extensions - kreuzverweis
				fromPattern = getPattern(matchesVar, filter.fromRangePatterns);
				toPattern = getPattern(matchesVar, filter.toRangePatterns);
				geoLatPattern = getPattern(matchesVar, filter.geoLatPatterns);
				geoLongPattern = getPattern(matchesVar, filter.geoLongPatterns);
				geoTolerancePattern = getPattern(matchesVar, filter.geoTolerancePatterns);
				// end GEO extensions - kreuzverweis
			}
			catch (IllegalArgumentException e) {
				failOrWarn(e);
				continue;
			}

			// check property restriction
			URI propertyURI = null;
			if (propertyPattern != null) {
				Var propertyVar = propertyPattern.getObjectVar();
				Value propertyValue = propertyVar.hasValue() ? propertyVar.getValue()
						: bindings.getValue(propertyVar.getName());

				if (propertyValue instanceof URI) {
					propertyURI = (URI)propertyValue;
				} else {
					failOrWarn(PROPERTY + " should have a property URI as object: " + propertyVar.getValue());
					continue;
				}
			}

			// fetch the query String
			String queryString = null;

			if (queryPattern != null) {
				Var queryVar = queryPattern.getObjectVar();
				Value queryValue = queryVar.hasValue() ? queryVar.getValue()
						: bindings.getValue(queryVar.getName());

				if (queryValue instanceof Literal) {
					queryString = ((Literal)queryValue).getLabel();
				}
			}
			
			// GEO extensions - kreuzverweis
			else if (fromPattern != null && toPattern != null) {
				Var fromVar = fromPattern.getObjectVar();
				Value fromValue = fromVar.hasValue() ? fromVar.getValue()
						: bindings.getValue(fromVar.getName());
				Var toVar = toPattern.getObjectVar();
				Value toValue = toVar.hasValue() ? toVar.getValue()
						: bindings.getValue(toVar.getName());
				if (fromValue instanceof Literal && toValue instanceof Literal) {

					// make sure to is really larger that from.
					String fromString = ((Literal)fromValue).getLabel();
					String toString = ((Literal)toValue).getLabel();
					if (toString.compareTo(fromString) < 0) {
						String buf = fromString;
						fromString = toString;
						toString = buf;
					}

					StringBuilder queryStringBuilder = new StringBuilder();
					queryStringBuilder.append("[");
					queryStringBuilder.append(fromString);
					queryStringBuilder.append(" TO ");
					queryStringBuilder.append(toString);
					queryStringBuilder.append("]");

					queryString = queryStringBuilder.toString();
				}
			} else if (geoLatPattern != null && geoLongPattern != null) {
				Var latVar = geoLatPattern.getObjectVar();
				Value latValue = latVar.hasValue() ? latVar.getValue()
						: bindings.getValue(latVar.getName());
				Var longVar = geoLongPattern.getObjectVar();
				Value longValue = longVar.hasValue() ? longVar.getValue()
						: bindings.getValue(longVar.getName());
				if (latValue instanceof Literal && longValue instanceof Literal) {
					Float latFrom, latTo, longFrom, longTo, tolerance;
					String latString = ((Literal)latValue).getLabel();
					Float lat = new Float(latString);
					String longString = ((Literal)longValue).getLabel();
					Float lon = new Float(longString);
					if (geoTolerancePattern != null) {
						Var tolVar = geoTolerancePattern.getObjectVar();
						Value tolValue = tolVar.hasValue() ? tolVar.getValue()
								: bindings.getValue(tolVar.getName());
						if (tolValue instanceof Literal) {
							String tol = ((Literal)tolValue).getLabel();
							tolerance = new Float(tol);
						} else {
							tolerance = 0F;
						}
					} else {
						tolerance = 0F;
					}
					latFrom = lat - tolerance;
					latTo = lat + tolerance;
					longFrom = lon - tolerance;
					longTo = lon + tolerance;
					String latFromStr = latFrom.toString();
					String latToStr = latTo.toString();
					String longFromStr = longFrom.toString();
					String longToStr = longTo.toString();
					String buf;
					if (latFromStr.compareTo(latToStr) > 0) {
						buf = latFromStr;
						latFromStr = latToStr;
						latToStr = buf;
					}
					if (longFromStr.compareTo(longToStr) > 0) {
						buf = longFromStr;
						longFromStr = longToStr;
						longToStr = buf;
					}
					StringBuilder qsb = new StringBuilder();
					qsb.append("http\\://www.w3.org/2003/01/geo/wgs84_pos#lat:[");
					qsb.append(latFrom);
					qsb.append(" TO ");
					qsb.append(latTo);
					qsb.append("] AND http\\://www.w3.org/2003/01/geo/wgs84_pos#long:[");
					qsb.append(longFrom);
					qsb.append(" TO ");
					qsb.append(longTo);
					qsb.append("]");
					queryString = qsb.toString();
				}
			}
			// end GEO exteions - kreuzverweis

			if (queryString == null) {
				failOrWarn("missing query string for Lucene query specification");
				continue;
			}
			
			// check the score variable, if any
			Var scoreVar = scorePattern == null ? null : scorePattern.getObjectVar();
			if (scoreVar != null && scoreVar.hasValue()) {
				failOrWarn(SCORE + " should have a variable as object: " + scoreVar.getValue());
				continue;
			}

			// check the snippet variable, if any
			Var snippetVar = snippetPattern == null ? null : snippetPattern.getObjectVar();
			if (snippetVar != null && snippetVar.hasValue()) {
				failOrWarn(SNIPPET + " should have a variable as object: " + snippetVar.getValue());
				continue;
			}

			// check type pattern
			if (typePattern == null) {
				logger.debug("Query variable '" + subject + "' has not rdf:type, assuming " + LUCENE_QUERY);
			}

			// register a QuerySpec with these details
//			result.add(new QuerySpec(matchesPattern, queryPattern, propertyPattern, scorePattern, snippetPattern, typePattern,
//					subject, queryString, propertyURI));
			// GEO entensions - modified - kreuzverweis
			result.add(new QuerySpec(matchesPattern, queryPattern, fromPattern, toPattern, geoLatPattern, geoLongPattern, geoTolerancePattern, propertyPattern, scorePattern, snippetPattern, typePattern,
					subject, queryString, propertyURI));
		}

		// fail on superflous typePattern, query, score, or snippet patterns.

		return result;
	}

	private void failOrWarn(Exception exception)
		throws StoreException
	{
		if (incompleteQueryFails) {
			throw exception instanceof StoreException ? (StoreException)exception : new StoreException(exception);
		}
		else {
			logger.warn(exception.getMessage(), exception);
		}
	}

	private void failOrWarn(String message)
		throws StoreException
	{
		if (incompleteQueryFails) {
			throw new StoreException("Invalid Text Query: " + message);
		}
		else {
			logger.warn(message);
		}
	}

	/**
	 * Returns the StatementPattern, if any, from the specified Collection that
	 * has the specified subject var. If multiple StatementPatterns exist with
	 * this subject var, an IllegalArgumentException is thrown. It also removes
	 * the patter from the arraylist, to be able to check if some patterns are
	 * added without a MATCHES property.
	 */
	private StatementPattern getPattern(Var subjectVar, ArrayList<StatementPattern> patterns)
		throws IllegalArgumentException
	{
		StatementPattern result = null;

		for (StatementPattern pattern : patterns) {
			if (pattern.getSubjectVar().equals(subjectVar)) {
				if (result == null) {
					result = pattern;
				}
				else {
					throw new IllegalArgumentException("multiple StatementPatterns with the same subject: "
							+ result + ", " + pattern);
				}
			}
		}
		// remove the result from the list, to filter out superflous patterns
		if (result != null)
			patterns.remove(result);
		return result;
	}

	private class PatternFilter extends QueryModelVisitorBase<RuntimeException> {

		public ArrayList<StatementPattern> typePatterns = new ArrayList<StatementPattern>();

		public ArrayList<StatementPattern> matchesPatterns = new ArrayList<StatementPattern>();

		public ArrayList<StatementPattern> queryPatterns = new ArrayList<StatementPattern>();

		public ArrayList<StatementPattern> propertyPatterns = new ArrayList<StatementPattern>();

		public ArrayList<StatementPattern> scorePatterns = new ArrayList<StatementPattern>();

		public ArrayList<StatementPattern> snippetPatterns = new ArrayList<StatementPattern>();

		// GEO extensions - kreuzverweis
		public ArrayList<StatementPattern> fromRangePatterns = new ArrayList<StatementPattern>();
		public ArrayList<StatementPattern> toRangePatterns = new ArrayList<StatementPattern>();
		public ArrayList<StatementPattern> geoLatPatterns = new ArrayList<StatementPattern>();
		public ArrayList<StatementPattern> geoLongPatterns = new ArrayList<StatementPattern>();
		public ArrayList<StatementPattern> geoTolerancePatterns = new ArrayList<StatementPattern>();
		// end GEO extensions - kreuzverweis

		/**
		 * Method implementing the visitor pattern that gathers all statements
		 * using a predicate from the LuceneSail's namespace.
		 */
		@Override
		public void meet(StatementPattern node) {
			
			Value predicate = node.getPredicateVar().getValue();

			if (MATCHES.equals(predicate)) {
				matchesPatterns.add(node);
			}
			else if (QUERY.equals(predicate)) {
				queryPatterns.add(node);
			}
			// GEO extensions - kreuzverweis
			else if (FROM.equals(predicate)) {
				fromRangePatterns.add(node);
			}
			else if (TO.equals(predicate)) {
				toRangePatterns.add(node);
			}
			else if (GEOLAT.equals(predicate)) {
				geoLatPatterns.add(node);
			}
			else if (GEOLONG.equals(predicate)) {
				geoLongPatterns.add(node);
			}
			else if (GEOTOLERANCE.equals(predicate)) {
				geoTolerancePatterns.add(node);
			}
			// end GEO extensions - kreuzverweis
			else if (PROPERTY.equals(predicate)) {
				propertyPatterns.add(node);
			}
			else if (SCORE.equals(predicate)) {
				scorePatterns.add(node);
			}
			else if (SNIPPET.equals(predicate)) {
				snippetPatterns.add(node);
			}
			else if (TYPE.equals(predicate)) {
				Value object = node.getObjectVar().getValue();
				if (LUCENE_QUERY.equals(object)) {
					typePatterns.add(node);
				}
			}
		}
	}
}
