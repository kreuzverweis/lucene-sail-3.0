/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.openrdf.cursor.Cursor;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.QueryModel;
import org.openrdf.query.algebra.SingletonSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.sail.SailConnection;
import org.openrdf.store.StoreException;

/**
 * The LuceneQueryIterator iterates over all permutations of the hits of each lucene query and
 * binds the respective variables to the respective values. The underlying sail evaluates this more
 * specific query against its store. This class contains code that was removed from the
 * LuceneSailConnection class.
 * @author Enrico Minack
 */
public class LuceneQueryIterator implements Cursor<BindingSet> {
	private final Logger log = LoggerFactory.getLogger(this.getClass());

	private final LuceneIndex index;			// the lucene index
	private final SailConnection sailConn;	// the wrapped sail connection
	private final Set<QuerySpec> queries;	// the lucene queries

	private final QueryModel query;		// the query tree
	private final BindingSet bindings;		// the initial binding set (as coming from the query)
	private final boolean includeInferred;	// the include-inferred statements flag

	private final Permutation permutations = new Permutation();		// provides all permutations of the hits of all lucene queries

	private Cursor<? extends BindingSet> nextBindingSets;		// the results iterator from the underlying sail 
	private BindingSet nextBindingSet = null;		// the next result from the results iterator
	private QueryBindingSet derivedBindings = null;	// the bindingset build up by this layer, taken to extend solutions from lower layer

	private final Map<QuerySpec, TopDocs> hits = new HashMap<QuerySpec, TopDocs>();		// maps the lucene query to its hits

	private final Formatter formatter = new SimpleHTMLFormatter();
	private final Map<QuerySpec, Highlighter> highlighters = new HashMap<QuerySpec, Highlighter>();

	public LuceneQueryIterator	(LuceneIndex index, SailConnection sailConn, Set<QuerySpec> queries, QueryModel query, BindingSet bindings, boolean includeInferred) throws StoreException {
		this.index = index;
		this.sailConn = sailConn;
		this.queries = queries;

		this.query = query;
		this.bindings = bindings;
		this.includeInferred = includeInferred;

		// initiate the evaluation
		evaluateLuceneQueries();
	}

	public boolean hasNext() {
		findNextBindingSet();
		return nextBindingSet != null;
	}

	public BindingSet next() {
		if(!hasNext())
			return null;
//			throw new NoSuchElementException();

		BindingSet result = nextBindingSet;
		this.nextBindingSet = null;
		return result;
	}

	public void remove() {

	}

	public void close() {
		hits.clear();
	}

	private void evaluateLuceneQueries() {
		// TODO: optimize lucene queries here
		// - if they refer to the same subject, merge them into one lucene query
		// - multiple different property constraints can be put into the lucene query string (escape colons here)
		for(QuerySpec query : this.queries) {
			if(this.hits.containsKey(query)) {
				log.warn("there are multiple lucene queries bound to the same resource!");
				log.warn("These queries should be merged here!");
				log.warn("This is not implemented yet!");
				log.warn("So this lucene query is ignored!");
				continue;
			}

			// evaluate the lucene query and put the hits into the map
			TopDocs hits = evaluate(query);
			this.hits.put(query, hits);

			// add the size of hits to the permutations
			this.permutations.addDigit(hits.totalHits);

			// finally remove the evaluated lucene query from the query tree
			removePatterns(query, this.query);
		}
	}

	/**
	 * Evaluates one Lucene Query. It distinguishes between two cases,
	 * the one where no subject is given and the one were it is given.
	 * @param query the lucene query to evaluate
	 * @return the lucene hits
	 */
	private TopDocs evaluate(QuerySpec query) {
		// get the subject of the query
		Resource subject = query.getSubject();

		try {
			// parse the query string to a lucene query
			Query lucenequery = this.index.parseQuery(query.getQueryString(), query.getPropertyURI());

			// if the query requests for the snippet, create a highlighter using this query
			if(query.getSnippetVariableName() != null) {
				Highlighter highlighter = new Highlighter(formatter, new QueryScorer(lucenequery));
				this.highlighters.put(query, highlighter);
			}

			// distinguish the two cases of subject == null
			if (subject == null) {
				return this.index.search(lucenequery);
			} else {
				return this.index.search(subject, lucenequery);
			}	
		}
		catch (Exception e) {
			log.error("There was a problem evaluating query '" + query.getQueryString() + "' for property '" + query.getPropertyURI() + "!", e);
		}

		return null;
	}

	/**
	 * Tries to find the next binding set (result). If no results iterator is existing,
	 * it tries to get one by calling {@link findNextBindingSets()}. If it succeeds,
	 * then this.nextBindingSet is not null, otherwise it is null.
	 */
	private void findNextBindingSet() {
		if(this.nextBindingSet != null)
			return;

		while(true) {
			if(this.nextBindingSets == null) {
				// fill nextBindingSets
				if(!findNextBindingSets())
					return;
			} else {
				// get next BindingSet
				this.nextBindingSet = getNextBindingSet();
			}

			if(this.nextBindingSet != null)
				return;
		}

	}

	/**
	 * Tries to find the next Bindings Set (results iterator) if there is none currently.
	 * It prepares the next permutation of hits, binds the respective variables and
	 * evaluates the query tree against the underlying sail. The results iterator is then
	 * stored as this.nextBindingSets. If this method fails to provide a next bindings set,
	 * it returns false.
	 * @return true if it succeeded, false otherwise
	 */
	private boolean findNextBindingSets() {
		// if there is still a next bindings set, we can safely return
		if(this.nextBindingSets != null)
			return true;

		// check if more permutations are available
		if(this.permutations.isInvalid())
			return false;

		// get the current permutation and the queries
		Vector<Integer> permutation = this.permutations.val();
		Iterator<QuerySpec> queries = this.queries.iterator();

		// this takes the new bindings
		derivedBindings = new QueryBindingSet();

		// for each digit ...
		for(Integer id : permutation) {
			// get the respective query (the query this digit stands for)
			if(!queries.hasNext()) {
				log.warn("There are more permutation digits then there are query specs!");
				return false;	// TODO: do we want to return true or false here?
			}
			QuerySpec query = queries.next();

			// if no hits are available, this binding set failed
			if(id <= 0)
				return false;

			// get the hit indicated by the digit value
			Document doc = getDoc(query, id-1);
			if(doc == null)
				return false;	// TODO: do we want to return true or false here?

			// get the score of the hit
			float score = getScore(query, id-1);

			// bind the respective variables
			String matchVar = query.getMatchesVariableName();
			if(matchVar != null) {
				Resource resource = this.index.getResource(doc);
				Value existing = derivedBindings.getValue(matchVar);
				// if the existing binding contradicts the current binding, than we can safely skip this permutation
				if((existing != null) && (!existing.stringValue().equals(resource.stringValue()))) {
					// invalidate the binding
					derivedBindings = null;

					// and exit the loop
					break;
				}
				derivedBindings.addBinding(matchVar, resource);
			}

			if((query.getScoreVariableName() != null) && (score > 0.0f))
				derivedBindings.addBinding(query.getScoreVariableName(), scoreToLiteral(score));

			if(query.getSnippetVariableName() != null) {
				// get the highlighter of this query
				Highlighter highlighter = this.highlighters.get(query);
				if(highlighter != null) {
					// extract snippets from
					// Lucene's query results
					StringBuffer result = new StringBuffer();

					// limit to the queried field, if there was one
					String fieldname = LuceneIndex.TEXT_FIELD_NAME;
					if (query.getPropertyURI() != null)
						fieldname = query.getPropertyURI().toString();
					Field[]  fields = doc.getFields(fieldname);
					int lastLen = 0;
					for(Field field: fields){
						String text = field.stringValue();
						TokenStream tokenStream = this.index.getAnalyzer().tokenStream(LuceneIndex.TEXT_FIELD_NAME, new StringReader(text));
						String next = "";
						try {
							next = highlighter.getBestFragments(tokenStream, text, 2, "...");
						}
						catch (IOException e) {
							log.error("IOException while getting snippet for filed " + field.name() + " for query\n" + query, e);
							continue;
						}
						catch (InvalidTokenOffsetsException e) {
							log.error("InvalidTokenOffsetsException while getting snippet for filed " + field.name() + " for query\n" + query, e);
							continue;
						}

						if(next.length() > 0){
							if(lastLen > 0){
								result.append("...");
							}
							lastLen = next.length();
							result.append(next);
						}
					}
					derivedBindings.addBinding(query.getSnippetVariableName(), new LiteralImpl(result.toString()));
				} else {
					log.warn("Lucene Query requests snippet, but no highlighter was generated for it, no snippets will be generated!\n{}", query);
				}
			}
		}

		// the derived bindings are used to extend the results of the following evaluation (the results do not contain the given bindings)
		// the bindings given to the LuceneSail shall not be included in its results, so we add them here, but won't include them in the results
		QueryBindingSet evaluateBindings = new QueryBindingSet(this.bindings);
		evaluateBindings.addAll(derivedBindings);

		// finally, evaluate the bindings against the underlying store
		try {
			if(derivedBindings != null) {
				this.nextBindingSets = this.sailConn.evaluate(query, derivedBindings, includeInferred);	
			}
		}
		catch (Exception e) {
			log.error("Provided sail connection could not evaluate tuple expression!", e);
			return false;		// TODO: do we want to return true or false here?
		}

		// go to the next permutation, if this was the last one,
		// invalidate the permutation instance, which will be check
		// at the beginning of the next call of the findNextBindingSets method
		if(this.permutations.next()) {
			this.permutations.invalidate();
		}

		// we succeeded
		return true;
	}

	/**
	 * Provides the next binding set (result) of the current results iterator
	 * @return a binding set, or null if it fails
	 */
	private BindingSet getNextBindingSet() {
		try {
			BindingSet possibleSolution = this.nextBindingSets.next();
			if(possibleSolution != null) {
				QueryBindingSet solution = new QueryBindingSet(possibleSolution);
				// fetch the next binding set given by the underlying sail and extend it with the derived bindings
				solution.addAll(derivedBindings);
				return solution;
			} else {
				this.nextBindingSets.close();
				this.nextBindingSets = null;
			}
		}
		catch (StoreException e) {
			log.error("Evaluation failed:", e);
		}

		return null;
	}

	/**
	 * Returns the lucene hit with the given id of the respective lucene query
	 * @param query the lucene query
	 * @param id the id of the hit to return
	 * @return the requested hit, or null if it fails
	 */
	private Document getDoc(QuerySpec query, int id) {
		try {
			return index.getIndexReader().document(this.hits.get(query).scoreDocs[id].doc);
		}
		catch (CorruptIndexException e) {
			log.error("The index seems to be corrupted:", e);
			return null;
		}
		catch (IOException e) {
			log.error("Could not read from index:", e);
			return null;
		}
	}

	/**
	 * Provides the score of the hit with the given id for the given lucenen query.
	 * @param query the lucene query
	 * @param id the id of the score to return
	 * @return the requested score, or 0.0f if it fails
	 */
	private float getScore(QuerySpec query, int id) {
		return this.hits.get(query).scoreDocs[id].score;
	}

	/**
	 * Returns a score value encoded as a Literal.
	 * @param score the float score to convert
	 * @return the score as a literal
	 */
	private Literal scoreToLiteral(float score) {
		return new LiteralImpl(String.valueOf(score), XMLSchema.FLOAT);
	}

	/**
	 * Removes the given pattern from the given tuple expression by
	 * replacing it with an singleton set.
	 * @param pattern the pattern to remove
	 * @param tupleExpr the tuple expression in which the pattern is to be removed
	 */
	private void remove(StatementPattern pattern, TupleExpr tupleExpr) {
		if (pattern != null) {
			pattern.replaceWith(new SingletonSet());
		}
	}

	/**
	 * Removes all StatementPatterns occurring in the given query from the given tuple expression.
	 * @param query the query to remove from the tuple expression
	 * @param query2.getTupleExpr() the tuple expression in which the query is to be removed
	 */
	private void removePatterns(QuerySpec query, QueryModel query2) {
		remove(query.getMatchesPattern(), query2.getTupleExpr());
		remove(query.getQueryPattern(), query2.getTupleExpr());
		remove(query.getScorePattern(), query2.getTupleExpr());
		remove(query.getPropertyPattern(), query2.getTupleExpr());
		remove(query.getSnippetPattern(), query2.getTupleExpr());
		remove(query.getTypePattern(), query2.getTupleExpr());
		// GEO extensions kreuzverweis
		remove(query.getFromPattern(), query2.getTupleExpr());
		remove(query.getToPattern(), query2.getTupleExpr());
		remove(query.getGeoTolerancePattern(), query2.getTupleExpr());
		remove(query.getGeoLatPattern(), query2.getTupleExpr());
		remove(query.getGeoLongPattern(), query2.getTupleExpr());
		// GEO extensions kreuzverweis
	}

}
