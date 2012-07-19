/*
 * Copyright Aduna, DFKI and L3S (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.openrdf.cursor.Cursor;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.store.StoreException;

/**
 * A LuceneIndex is a one-stop-shop abstraction of a Lucene index. It takes care
 * of proper synchronization of IndexReaders, IndexWriters and IndexSearchers in
 * a way that is suitable for a LuceneSail.
 */
public class LuceneIndex {



	/**
	 * A utility FieldSelector that only selects the ID field to be loaded.
	 * Useful when locating matching Resources in a LuceneIndex and the other
	 * Document fields are not required.
	 */
	public static FieldSelector ID_FIELD_SELECTOR = new FieldSelector() {

		private static final long serialVersionUID = 4302925811117170860L;

		public FieldSelectorResult accept(String fieldName) {
			return fieldName.equals(ID_FIELD_NAME) ? FieldSelectorResult.LOAD : FieldSelectorResult.NO_LOAD;
		}
	};

	/**
	 * The name of the Document field holding the Resource identifier. The value
	 * stored in this field is either a URI or a BNode ID.
	 */
	public static final String ID_FIELD_NAME = "id";

	/**
	 * The name of the Document field that holds multiple text values of a
	 * Resource. The field is called "text", as it contains all text,
	 * but was called "ALL" during the discussion. 
	 * For each statement-literal of the resource, 
	 * the object literal is stored in a field using the predicate-literal 
	 * and additionally in a TEXT_FIELD_NAME-literal field.
	 * The reasons are given in the documentation of {@link #addProperty(String, String, Document)}
	 */
	public static final String TEXT_FIELD_NAME = "text";

	/**
	 * The name of the Document field holding the context identifer(s).
	 */
	public static final String CONTEXT_FIELD_NAME = "context";

	/**
	 * the null context
	 */
	public static final String CONTEXT_NULL = "null";


	/**
	 * String used to prefix BNode IDs with so that we can distinguish BNode
	 * fields from URI fields in Documents. The prefix is chosen so that it is
	 * invalid as a (part of a) URI scheme.
	 */
	public static final String BNODE_ID_PREFIX = "!";

	static {
		BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
	}

	private Logger logger = LoggerFactory.getLogger(getClass());

	/**
	 * The Directory that holds the Lucene index files.
	 */
	private Directory directory;

	/**
	 * The Analyzer used to tokenize strings and queries.
	 */
	private Analyzer analyzer;

	/**
	 * The QueryParser used to parse Lucene query Strings into Query instances.
	 */
	private QueryParser queryParser;

	/**
	 * IndexSearcher that can be used to read the current index' contents.
	 * Created lazily.
	 */
	private IndexReader indexReader;

	/**
	 * The IndexSearcher that can be used to query the current index' contents.
	 * Created lazily.
	 */
	private IndexSearcher indexSearcher;

	/**
	 * The IndexWriter that can be used to alter the index' contents. Created
	 * lazily.
	 */
	private IndexWriter indexWriter;

	/**
	 * Creates a new LuceneIndex.
	 * 
	 * @param directory
	 *        The Directory in which an index can be found and/or in which index
	 *        files are written.
	 * @param analyzer
	 *        The Analyzer that will be used for tokenizing strings to index and
	 *        queries.
	 * @throws IOException
	 *         When the Directory could not be unlocked.
	 */
	public LuceneIndex(Directory directory, Analyzer analyzer)
	throws IOException
	{
		this.directory = directory;
		this.analyzer = analyzer;

		this.queryParser = new QueryParser(Version.LUCENE_CURRENT, TEXT_FIELD_NAME, analyzer);
		// enable scoring for wildcard queries
		//this.queryParser.setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE); 
		
		// get rid of any locks that may have been left by previous (crashed)
		// sessions
		if (IndexWriter.isLocked(directory)) {
			logger.warn("unlocking directory " + directory);
			IndexWriter.unlock(directory);
		}

		// do some initialization for new indices
		if (!IndexReader.indexExists(directory)) {
			IndexWriter writer = new IndexWriter(directory, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED);
			writer.setUseCompoundFile(true);
			writer.setMaxFieldLength(Integer.MAX_VALUE);
			writer.close();
		}
	}

	// //////////////////////////////// Setters and getters

	public Directory getDirectory() {
		return directory;
	}

	public Analyzer getAnalyzer() {
		return analyzer;
	}

	// //////////////////////////////// Methods for controlled index access

	public IndexReader getIndexReader()
	throws IOException
	{
		if (indexReader == null) {
			indexReader = IndexReader.open(directory, false);
		}
		return indexReader;
	}

	public IndexSearcher getIndexSearcher()
	throws IOException
	{
		if (indexSearcher == null) {
			IndexReader reader = getIndexReader();
			indexSearcher = new IndexSearcher(reader);
		}
		return indexSearcher;
	}

	public IndexWriter getIndexWriter()
	throws IOException
	{

		if (indexWriter == null) {
			indexWriter = new IndexWriter(directory, analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
		}
		return indexWriter;
	}

	public void shutDown()
	throws IOException
	{
		// try-finally setup ensures that closing of an instance is not skipped
		// when an earlier instance resulted in an IOException
		// FIXME: is there a more elegant way to ensure this?
		try {
			try {
				if (indexSearcher != null) {
					indexSearcher.close();
				}
			}
			finally {
				try {
					if (indexReader != null) {
						indexReader.close();
					}
				}
				finally {
					if (indexWriter != null) {
						indexWriter.close();
					}
				}
			}
		}
		finally {
			indexSearcher = null;
			indexReader = null;
			indexWriter = null;
		}
	}

	// //////////////////////////////// Methods for updating the index

	/**
	 * Indexes the specified Statement.
	 */
	public synchronized void addStatement(Statement statement)
	throws IOException
	{
		// determine stuff to store
		Value object = statement.getObject();
		if (!(object instanceof Literal)) {
			return;
		}

		String field = statement.getPredicate().toString();
		String text = ((Literal)object).getLabel();
		String context = (statement.getContext()!=null)? getID(statement.getContext()) : null;
		boolean updated = false;
		IndexWriter writer = null;

		// fetch the Document representing this Resource
		String id = getID(statement.getSubject());
		Term idTerm = new Term(ID_FIELD_NAME, id);
		Document document = getDocument(idTerm);

		if (document == null) {
			// there is no such Document: create one now
			document = new Document();
			addID(id, document);
			addProperty(field, text, document);
			// add context
			addContext(context, document, false);

			// add it to the index
			writer = getIndexWriter();
			writer.addDocument(document);
			updated = true;
		}
		else {
			// update this Document when this triple has not been stored already
			if (!hasProperty(field, text, document)) {
				// create a copy of the old document; updating the retrieved
				// Document instance works ok for stored properties but indexed data
				// gets lots when doing an IndexWriter.updateDocument with it
				Document newDocument = new Document();

				// add all existing fields (including id, context, and text)
				for (Object oldFieldObject : document.getFields()) {
					Field oldField = (Field)oldFieldObject;
					newDocument.add(oldField);
				}

				// add new context, if not present
				addContext(context, newDocument, true);

				// add the new triple to the cloned document
				addProperty(field, text, newDocument);

				// update the index with the cloned document
				writer = getIndexWriter();
				writer.updateDocument(idTerm, newDocument);
				updated = true;
			}
		}

		if (updated) {
			// make sure that these updates are visible for new
			// IndexReaders/Searchers
			writer.commit();

			// the old IndexReaders/Searchers are not outdated
			invalidateReaders();
		}
	}

	/**
	 * Add the "context" value to the doc
	 * @param context the context or null, if null-context
	 * @param document the document
	 * @param ifNotExists check if this context exists
	 */
	private void addContext(String context, Document document, boolean ifNotExists) {
		if (context != null)
		{
			if (ifNotExists)
			{
				Field[] fields = document.getFields(CONTEXT_FIELD_NAME);
				for (Field f : fields) {
					if (context.equals(f.stringValue()))
						return;
				}
			}
			document.add(new Field(CONTEXT_FIELD_NAME, context, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
		}
	}

	/**
	 * Returns the String ID corresponding with the specified Resource.
	 * The id string is either the URI or a bnode prefixed with a "!".
	 */
	private String getID(Resource resource) {
		if (resource instanceof URI) {
			return resource.toString();
		}
		else if (resource instanceof BNode) {
			return BNODE_ID_PREFIX + ((BNode)resource).getID();
		}
		else if(resource == null) {
			return "null";
		}
		else {
			throw new IllegalArgumentException("Unknown Resource type: " + resource);
		}
	}

	/**
	 * Get the ID for a context.
	 * Context can be null, then the "null" string is returned
	 * @param resource the context 
	 * @return a string
	 */
	private String getContextID(Resource resource) {
		if (resource == null)
			return CONTEXT_NULL;
		else return getID(resource);
	}

	/**
	 * Returns a Document representing the specified Resource, or null when no
	 * such Document exists yet.
	 */
	private Document getDocument(Term term)
	throws IOException
	{
		IndexReader reader = getIndexReader();
		TermDocs termDocs = reader.termDocs(term);

		try {
			if (termDocs.next()) {
				// return the Document and make sure there are no others
				int docNr = termDocs.doc();
				if (termDocs.next()) {
					throw new RuntimeException("Multiple Documents for resource " + term.text());
				}

				return reader.document(docNr);
			}
			else {
				// no such Document
				return null;
			}
		}
		finally {
			termDocs.close();
		}
	}

	/**
	 * Returns a Document representing the specified Resource, or null when no
	 * such Document exists yet.
	 */
	public Document getDocument(Resource subject)
	throws IOException
	{
		//	 fetch the Document representing this Resource
		String id = getID(subject);
		Term idTerm = new Term(ID_FIELD_NAME, id);
		return getDocument(idTerm);
	}

	/**
	 * Checks whether a field occurs with a specified value in a Document.
	 */
	private boolean hasProperty(String fieldName, String value, Document document) {
		Field[] fields = document.getFields(fieldName);
		if (fields != null) {
			for (Field field : fields) {
				if (value.equals(field.stringValue())) {
					return true;
				}
			}
		}

		return false;
	}

	/**
	 * Determines whether the specified field name is a property field name.
	 */
	private boolean isPropertyField(String fieldName) {
		return !ID_FIELD_NAME.equals(fieldName) && !TEXT_FIELD_NAME.equals(fieldName)
		&& !CONTEXT_FIELD_NAME.equals(fieldName);
	}

	/**
	 * Determines the number of properties stored in a Document.
	 */
	private int numberOfPropertyFields(Document document) {
		// count the properties that are NOT id nor context nor text
		int propsize = 0;
		for (Object o : document.getFields())
		{
			Field f = (Field)o;
			if (isPropertyField(f.name()))
				propsize++;
		}
		return propsize;
	}

	/**
	 * Stores and indexes an ID in a Document.
	 */
	private void addID(String id, Document document) {
		document.add(new Field(ID_FIELD_NAME, id, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
	}

	/**
	 * check if the passed statement should be added (is it indexed? is it stored?)
	 * and add it as predicate to the passed document.
	 * No checks whether the predicate was already there.
	 * @param statement the statement to add
	 * @param document the document to add to
	 */
	private void addProperty(Statement statement, Document document) {
		Value object = statement.getObject();
		if (!(object instanceof Literal)) {
			return;
		}
		String field = statement.getPredicate().toString();
		String text = ((Literal)object).getLabel();
		addProperty(field, text, document);
	}

	/**
	 * Stores and indexes a property in a Document.
	 * We don't have to recalculate the concatenated text: just add another
	 * TEXT field and Lucene will take care of this. Additional advantage:
	 *	Lucene may be able to handle the invididual strings in a way that may
	 *	affect e.g. phrase and proximity searches (concatenation basically
	 *	means loss of information).
	 *	NOTE: The TEXT_FIELD_NAME has to be stored, see in LuceneSail
	 *	@see LuceneSail#storedindexed
	 */
	private void addProperty(String predicate, String text, Document document) {
		// store this predicate
		document.add(new Field(predicate, text, Field.Store.YES, Field.Index.ANALYZED));

		// and in TEXT_FIELD_NAME
		document.add(new Field(TEXT_FIELD_NAME, text, Field.Store.YES, Field.Index.ANALYZED));
	}

	private void invalidateReaders()
	throws IOException
	{
		try {
			try {
				if (indexSearcher != null) {
					indexSearcher.close();
				}
			}
			finally {
				if (indexReader != null) {
					indexReader.close();
				}
			}
		}
		finally {
			indexSearcher = null;
			indexReader = null;
		}
	}

	public synchronized void removeStatement(Statement statement)
	throws IOException
	{
		Value object = statement.getObject();
		if (!(object instanceof Literal)) {
			return;
		}

		IndexWriter writer = null;
		boolean updated = false;

		// fetch the Document representing this Resource
		String id = getID(statement.getSubject());
		Term idTerm = new Term(ID_FIELD_NAME, id);
		Document document = getDocument(idTerm);

		if (document != null) {
			// determine the values used in the index for this triple
			String fieldName = statement.getPredicate().toString();
			String text = ((Literal)object).getLabel();

			// see if this triple occurs in this Document
			if (hasProperty(fieldName, text, document)) {
				// if the Document only has one predicate field, we can remove the
				// document
				int nrProperties = numberOfPropertyFields(document);
				if (nrProperties == 0) {
					logger.warn("encountered document with zero properties, should have been deleted: " + id);
				}
				else if (nrProperties == 1) {
					writer = getIndexWriter();
					writer.deleteDocuments(idTerm);
					updated = true;
				}
				else {
					// there are more triples encoded in this Document: remove the
					// document and add a new Document without this triple
					Document newDocument = new Document();
					addID(id, newDocument);

					for (Object oldFieldObject : document.getFields()) {
						Field oldField = (Field)oldFieldObject;
						String oldFieldName = oldField.name();
						String oldValue = oldField.stringValue();

						if (isPropertyField(oldFieldName)
								&& !(fieldName.equals(oldFieldName) && text.equals(oldValue)))
						{
							addProperty(oldFieldName, oldValue, newDocument);
						}
					}

					writer = getIndexWriter();
					writer.updateDocument(idTerm, newDocument);
					updated = true;
				}
			}
		}

		if (updated) {
			// make sure that these updates are visible for new
			// IndexReaders/Searchers
			writer.commit();

			// the old IndexReaders/Searchers are not outdated
			invalidateReaders();
		}
	}

	/**
	 * Commits any changes done to the LuceneIndex since the last commit. The
	 * semantics is synchronous to SailConnection.commit(), i.e. the LuceneIndex
	 * should be committed/rollbacked whenever the LuceneSailConnection is
	 * committed/rollbacked.
	 */
	public void commit()
	throws IOException
	{
		// FIXME: implement Need to make sure, only one commit happens.
	}

	public void rollback()
	throws IOException
	{
		// FIXME: implement Need to make sure, only one commit happens.
	}

	// //////////////////////////////// Methods for querying the index

	/**
	 * Returns the Resource corresponding with the specified Document number.
	 * Note that all of Lucene's restrictions of using document numbers apply.
	 */
	public Resource getResource(int documentNumber) throws IOException {
		Document document = getIndexSearcher().doc(documentNumber, ID_FIELD_SELECTOR);
		return document == null ? null : getResource(document);
	}

	/**
	 * Returns the Resource corresponding with the specified Document.
	 */
	public Resource getResource(Document document) {
		String idString = document.get(ID_FIELD_NAME);
		return getResource(idString);
	}

	/**
	 * Parses an id-string (a serialized resource) back to a resource
	 * Inverse method of {@link #getID(Resource)}
	 * @param idString
	 */
	private Resource getResource(String idString) {
		if (idString.startsWith(BNODE_ID_PREFIX)) {
			return new BNodeImpl(idString.substring(BNODE_ID_PREFIX.length()));
		}
		else {
			return new URIImpl(idString);
		}	
	}

	/**
	 * Evaluates the given query on this LuceneIndex and passes all results to
	 * the LuceneResultListener. A FieldSelector can optionally be specified,
	 * which can reduce the performance costs of retrieving the Documents.
	 */
	public void search(String query, SearchResultListener resultListener, FieldSelector fieldSelector)
	throws ParseException, IOException
	{
		search(queryParser.parse(query), resultListener, fieldSelector);
	}

	/**
	 * Evaluates the given query on this LuceneIndex and passes all results to
	 * the LuceneResultListener. A FieldSelector can optionally be specified,
	 * which can reduce the performance costs of retrieving the Documents.
	 */
	private void search(Query query, final SearchResultListener resultListener,
			final FieldSelector fieldSelector)
	throws IOException
	{
		// create a Collector that will redirect results to a SearchResultListener
		final IndexSearcher searcher = getIndexSearcher();

		AllDocsCollector collector = new AllDocsCollector();

		// start querying
		try {
			searcher.search(query, collector);
			for (ScoreDoc scoreDoc: collector.getTopDocs().scoreDocs) {
				resultListener.handleResult(
						searcher.doc(scoreDoc.doc, fieldSelector), scoreDoc.score);
			}
		}
		catch (IOException e) {
			// IOExceptions are rethrown, all others are simply logged
			throw e;
		}
		catch (Exception e) {
			logger.error("Exception while evaluating Lucene query: " + query, e);
		}
	}

	/**
	 * Evaluates the given query and returns the results as a Hits instance.
	 */
	//	public Hits search(String query)
	//		throws ParseException, IOException
	//	{
	//		return search(queryParser.parse(query));
	//	}

	/**
	 * Evaluates the given query only for the given resource.
	 */
	public TopDocs search(Resource resource, Query query)
	throws ParseException, IOException
	{
		// rewrite the query
		TermQuery idQuery = new TermQuery(new Term(ID_FIELD_NAME, getID(resource)));
		BooleanQuery combinedQuery = new BooleanQuery();
		combinedQuery.add(idQuery, Occur.MUST);
		combinedQuery.add(query, Occur.MUST);
		AllDocsCollector collector = new AllDocsCollector();
		getIndexSearcher().search(combinedQuery, collector);
		TopDocs hits = collector.getTopDocs();

		if(hits.totalHits > 1)
			logger.warn("More than one Lucene doc was found with {} == {}", ID_FIELD_NAME, getID(resource));

		return hits;
	}

	/**
	 * Parse the passed query.
	 * @param query string
	 * @return the parsed query
	 * @throws ParseException when the parsing brakes
	 */
	public Query parseQuery(String query, URI propertyURI) throws ParseException {
		Query parsedQuery = getQueryParser(propertyURI).parse(query);
		// Enable Scoring for range queries. 
		// TODO: Make this configurable, as scoring takes CPU time.
//		if (parsedQuery instanceof MultiTermQuery) {
//			MultiTermQuery multiTermQuery = (MultiTermQuery) parsedQuery;
//			multiTermQuery.setRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE);
//		}
		return parsedQuery;
	}

	/**
	 * Evaluates the given query and returns the results as a Hits instance.
	 */
	// FIXME Use collector and do not fix number of hits
	public TopDocs search(Query query)
	throws IOException
	{
		AllDocsCollector collector = new AllDocsCollector();
		getIndexSearcher().search(query, collector);
		return collector.getTopDocs();
	}

	// never used.
//	
//	/**
//	 * Gets the score for a particular Resource and query. Returns a value < 0
//	 * when the Resource does not match the query.
//	 */
//	public float getScore(Resource resource, String query, URI propertyURI)
//	throws ParseException, IOException
//	{
//		return getScore(resource, getQueryParser(propertyURI).parse(query));
//	}
//
//	/**
//	 * Gets the score for a particular Resource and query. Returns a value < 0
//	 * when the Resource does not match the query.
//	 */
//	public float getScore(Resource resource, Query query)
//	throws IOException
//	{
//		// rewrite the query
//		TermQuery idQuery = new TermQuery(new Term(ID_FIELD_NAME, getID(resource)));
//		BooleanQuery combinedQuery = new BooleanQuery();
//		combinedQuery.add(idQuery, Occur.MUST);
//		combinedQuery.add(query, Occur.MUST);
//		IndexSearcher searcher = getIndexSearcher();
//
//		// fetch the score when the URI matches the original query
//		TopDocs docs = searcher.search(combinedQuery, null, 1);
//		if (docs.totalHits == 0) {
//			return -1f;
//		}
//		else {
//			return docs.scoreDocs[0].score;
//		}
//	}

	private QueryParser getQueryParser(URI propertyURI) {
		// check out which query parser to use, based on the given property URI
		if(propertyURI == null)
			// if we have no property given, we take the default query parser which has the TEXT_FIELD_NAME as the default field
			return queryParser;
		else {
			// otherwise we take a new query parser that has the given property as the default field 
			QueryParser qp = new QueryParser(Version.LUCENE_CURRENT, propertyURI.toString(), this.analyzer);
//			qp.setMultiTermRewriteMethod(MultiTermQuery.SCORING_BOOLEAN_QUERY_REWRITE); 
			return qp;
		}
	}

//	private class ExceptionWrapper extends RuntimeException {
//
//		private static final long serialVersionUID = 3207549343445198139L;
//
//		public ExceptionWrapper(Exception wrappedException) {
//			super(wrappedException);
//		}
//
//		@Override
//		public Exception getCause() {
//			return (Exception)super.getCause();
//		}
//	}

	/**
	 * Add many statements at the same time, remove many statements at the same time.
	 * Ordering by resource has to be done inside this method.
	 * The passed added/removed sets are disjunct, no statement can be in both
	 * @param added all added statements, can have multiple subjects
	 * @param removed all removed statements, can have multiple subjects
	 */
	public synchronized void addRemoveStatements(Collection<Statement> added, Collection<Statement> removed, Sail sail) throws Exception {
		// Buffer per resource
		ListMap<Resource, Statement> rsAdded = new ListMap<Resource, Statement>();
		ListMap<Resource, Statement> rsRemoved = new ListMap<Resource, Statement>();
		HashSet<Resource> resources = new HashSet<Resource>();
		for (Statement s : added) {
			rsAdded.put(s.getSubject(), s);
			resources.add(s.getSubject());
		}
		for (Statement s : removed) {
			rsRemoved.put(s.getSubject(), s);
			resources.add(s.getSubject());
		}

		IndexWriter writer = getIndexWriter();

		// for each resource, add/remove
		for (Resource resource : resources) {
			// is the resource in the store?

			//	fetch the Document representing this Resource
			String id = getID(resource);
			Term idTerm = new Term(ID_FIELD_NAME, id);
			Document document = getDocument(idTerm);

			if (document == null) {
				// there is no such Document: create one now
				document = new Document();
				addID(id, document);
				// add all statements, remember the contexts 
				HashSet<Resource> contextsToAdd = new HashSet<Resource>();
				List<Statement> list = rsAdded.get(resource);
				if (list != null)
					for (Statement s : list)
					{
						addProperty(s, document);
						contextsToAdd.add(s.getContext());
					}
				// add all contexts
				for (Resource c : contextsToAdd)
				{
					addContext(getContextID(c), document, false);
				}

				// add it to the index
				writer.addDocument(document);

				// THERE SHOULD BE NO DELETED TRIPLES ON A NEWLY ADDED RESOURCE
				if (rsRemoved.containsKey(resource))
					logger.warn(rsRemoved.get(resource).size()
							+" statements are marked to be removed that should not be in the store, for resource "
							+resource+". Nothing done.");
			}
			else {
				// update the Document 

				// create a copy of the old document; updating the retrieved
				// Document instance works ok for stored properties but indexed data
				// gets lots when doing an IndexWriter.updateDocument with it
				//				Document newDocument = new Document();

				// buffer the removed literal statements
				ListMap<String, String> removedOfResource = null;
				{
					List<Statement> removedStatements = rsRemoved.get(resource);
					if (removedStatements != null)
					{ 
						removedOfResource = new ListMap<String, String>();
						for (Statement r : removedStatements) {
							if (r.getObject() instanceof Literal)
								removedOfResource.put(r.getPredicate().toString(), ((Literal)r.getObject()).getLabel());
						}
					}
				}

				//				// add all existing fields (including id, context, and text)
				//				// but without adding the removed ones
				//				for (Object oldFieldObject : document.getFields()) {
				//					Field oldField = (Field)oldFieldObject;
				//					// do not copy removed statements to the new version of the document
				//					if (removedOfResource != null)
				//					{
				//						// which fields were removed?
				//						List<String> objectsRemoved = removedOfResource.get(oldField.name());
				//						//logger.info("removed of resource: " + objectsRemoved + " oldVal: " + oldField.stringValue() + " oldName: " + oldField.name());
				//						// FIXME: Need to remove context entry, if this is the last statement in the context.
				//						if ((objectsRemoved != null ) && 
				//								objectsRemoved.contains(oldField.stringValue())
				//								// by Simon:
				//								|| CONTEXT_FIELD_NAME.equals(oldField.name())
				//								//
				//								)
				//							continue;
				//					}
				//					logger.info("readding: " + oldField);
				//					newDocument.add(oldField);
				//				}

				// for Resources that had deletions, readd from store.
				SailConnection con = sail.getConnection();
				try {
					Cursor<? extends Statement> it = con.getStatements(resource, null, null, false);
					Statement s = it.next();
					while (s != null) {
						rsAdded.put(resource, s);
						s = it.next();
					}
				} finally {
					con.close();
				}

				//				// add all statements to this document, remember the contexts 
				//				{
				//					List<Statement> addedToResource = rsAdded.get(resource);
				//					if (addedToResource != null)
				//					{
				//						HashSet<Resource> contextsToAdd = new HashSet<Resource>();
				//						for (Statement s : addedToResource)
				//						{
				//							addProperty(s, newDocument);
				//							contextsToAdd.add(s.getContext());
				//						}
				//						// add all contexts
				//						for (Resource c : contextsToAdd)
				//							addContext(getContextID(c), newDocument, false);
				//					}
				//				}
				//				
				//
				//				// update the index with the cloned document
				//				writer.updateDocument(idTerm, newDocument);
				writer.deleteDocuments(idTerm);
				List<Statement> toAdd = rsAdded.get(resource);
				logger.debug("reindex resource: " + resource + " instance of " + resource.getClass());
				if (toAdd != null) addDocument((URI)resource, toAdd);
			}
		}
		// make sure that these updates are visible for new
		// IndexReaders/Searchers
		writer.commit();

		// the old IndexReaders/Searchers are not outdated
		invalidateReaders();

	}

	/**
	 * @param contexts
	 * @param sail - the underlying native sail where to read the missing triples from 
	 * after deletion
	 * @throws StoreException 
	 * @throws SailException 
	 */
	public synchronized void clearContexts(Resource[] contexts, Sail sail) throws IOException, StoreException {
		logger.debug("deleting contexts: "+contexts);
		// these resources have to be read from the underlying rdf store
		// and their triples have to be added to the luceneindex after deletion of documents
		HashSet<Resource> resourcesToUpdate = new HashSet<Resource>();

		// remove all contexts passed
		for (Resource context : contexts) 
		{
			String contextString = getID(context);
			Term contextTerm = new Term(CONTEXT_FIELD_NAME, contextString);
			IndexReader reader = getIndexReader();

			// now check all documents, and remember the URI of the resources
			// that were in multiple contexts
			TermDocs termDocs = reader.termDocs(contextTerm);
			try {
				while (termDocs.next()) {
					Document document = reader.document(termDocs.doc());
					// does this document have any other contexts?
					Field[] fields = document.getFields(CONTEXT_FIELD_NAME);
					for (Field f : fields) 
					{
						if (!contextString.equals(f.stringValue())&&!f.stringValue().equals("null")) // there is another context
						{
							logger.debug("test new contexts: "+f.stringValue());
							// is it in the also contexts (lucky us if it is)
							Resource otherContextOfDocument = getResource(f.stringValue());
							boolean isAlsoDeleted = false;
							for (Resource c: contexts){
								if (c.equals(otherContextOfDocument))
									isAlsoDeleted = true;
							}
							// the otherContextOfDocument is now eihter marked for deletion or not
							if (!isAlsoDeleted) {
								// get ID of document
								Resource r = getResource(document);
								resourcesToUpdate.add(r);							
							}
						}
					}
				}
			} finally {
				termDocs.close();
			}

			// now delete all documents from the deleted context
			getIndexWriter().deleteDocuments(contextTerm);
		}

		// now add those again, that had other contexts also.
		SailConnection con = sail.getConnection();
		try {
			// for each resource, add all
			for (Resource resource : resourcesToUpdate) {
				logger.debug("re-adding resource "+resource);
				ArrayList<Statement> toAdd = new ArrayList<Statement>();
				Cursor<? extends Statement> it = con.getStatements(resource, null, null, false);
				Statement s = it.next();
				while (s != null) {
					toAdd.add(s);
					s = it.next();
				}
				addDocument(resource, toAdd);
			}
		} finally {
			con.close();
		}
		getIndexWriter().commit();
		invalidateReaders();

	}


	/**
	 * Add a complete Lucene Document based on these statements.
	 * Do not search for an existing document with the same subject id.
	 * (assume the existing document was deleted)
	 * @param statements the statements that make up the resource
	 * @throws IOException 
	 */
	public synchronized void addDocument(Resource subject, List<Statement> statements) throws IOException {
		// create a new document
		Document document = new Document();
		String id = getID(subject);
		addID(id, document);

		// contexts
		HashSet<Resource> contexts = new HashSet<Resource>();

		for (Statement statement : statements) {
			// determine stuff to store
			addProperty(statement, document);
			contexts.add(statement.getContext());
		}

		// contexts
		for (Resource context : contexts) 
			// add context
			addContext(getID(context), document, false);

		// add it to the index
		IndexWriter writer = getIndexWriter();
		writer.addDocument(document);
	}


	/**
	 * 
	 */
	public synchronized void clear()  throws IOException  {
		// clear
		// the old IndexReaders/Searchers are not outdated
		invalidateReaders();
		if (indexWriter != null)
			indexWriter.close();

		// crate new writer
		indexWriter = new IndexWriter(directory, analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
		indexWriter.close();
		indexWriter = null;

	}

	private class AllDocsCollector extends Collector {

		List<ScoreDoc> scoreDocs = new LinkedList<ScoreDoc>();
		//		IndexReader reader = null;
		Scorer scorer = null;
		float maxScore = 0;
		int docBase = 0;

		@Override
		public boolean acceptsDocsOutOfOrder() {
			return false;
		}

		@Override
		public void collect(int doc)
		throws IOException
		{
			if (scorer.score() > maxScore) maxScore = scorer.score();
			ScoreDoc scoreDoc = new ScoreDoc(doc+docBase, scorer.score());
			scoreDocs.add(scoreDoc);
		}

		@Override
		public void setNextReader(IndexReader reader, int docBase)
		throws IOException
		{
			this.docBase = docBase;
//			System.out.println("new reader: " + reader + ", rebase: " + docBase);
		}

		@Override
		public void setScorer(Scorer scorer)
		throws IOException
		{
			this.scorer = scorer;
		}

		public TopDocs getTopDocs() {
			return new TopDocs(scoreDocs.size(), scoreDocs.toArray(new ScoreDoc[0]), maxScore);
		}

	}
}
