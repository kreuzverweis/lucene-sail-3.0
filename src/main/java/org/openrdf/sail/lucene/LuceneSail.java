/*
 * Copyright Aduna, DFKI and L3S (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.openrdf.cursor.Cursor;
import org.openrdf.model.Statement;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.StackableSail;
import org.openrdf.sail.helpers.NotifyingSailWrapper;
import org.openrdf.store.StoreException;

/**
 * A LuceneSail wraps an arbitrary existing Sail and extends it with support for
 * full-text search on all Literals.
 * 
 * <h2>Setting up a LuceneSail</h2>
 * LuceneSail works in two modes: storing its data into a directory on the harddisk
 * or into a RAMDirectory in RAM (which is discarded when the program ends).
 * 
 * Example with storage in a folder:
 * <code>
   // create a sesame memory sail
	MemoryStore memoryStore = new MemoryStore();

	// create a lucenesail to wrap the memorystore
	LuceneSail lucenesail = new LuceneSail();		
	// set this parameter to store the lucene index on disk
	lucenesail.setParameter(LuceneSail.LUCENE_DIR_KEY, "./data/mydirectory");
	
	// wrap memorystore in a lucenesail
	lucenesail.setBaseSail(memoryStore);
	
	// create a Repository to access the sails
	SailRepository repository = new SailRepository(lucenesail);
	repository.initialize();
	</code>
 * Example with storage in a RAM directory:
 * <code>
   // create a sesame memory sail
	MemoryStore memoryStore = new MemoryStore();

	// create a lucenesail to wrap the memorystore
	LuceneSail lucenesail = new LuceneSail();		
   // set this parameter to let the lucene index store its data in ram
	lucenesail.setParameter(LuceneSail.LUCENE_RAMDIR_KEY, "true");
	
	// wrap memorystore in a lucenesail
	lucenesail.setBaseSail(memoryStore);
	
	// create a Repository to access the sails
	SailRepository repository = new SailRepository(lucenesail);
	repository.initialize();
	</code>
 * 
 * <h2>Asking full-text queries</h2>
 * Text queries are expressed using the virtual properties of the LuceneSail.
 * An example query looks like this (SERQL):
 * <code>
 * SELECT Subject, Score, Snippet 
 * FROM {Subject} <http://www.openrdf.org/contrib/lucenesail#matches> {} 
 * <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> {<http://www.openrdf.org/contrib/lucenesail#LuceneQuery>}; 
 * <http://www.openrdf.org/contrib/lucenesail#query> {"my Lucene query"}; 
 * <http://www.openrdf.org/contrib/lucenesail#score> {Score}; 
 * <http://www.openrdf.org/contrib/lucenesail#snippet> {Snippet} 
 * </code>
 * 
 * When defining queries, these properties <b>type and query are mandatory</b>.
 * Also, the <b>matches relation is mandatory</b>. When one of these misses,
 * the query will not be executed as expected.
 * The failure behavior can be configured, setting the Sail property 
 * "incompletequeryfail" to true will throw a SailException when such patterns
 * are found, this is the default behavior to help finding inaccurate queries.
 * Set it to false to have warnings logged instead.
 * 
 * <b>Multiple queries</b> can be issued to the sail, the results of the queries will
 * be integrated. Note that you cannot use the same variable for multiple
 * Text queries, if you want to combine text searches, use Lucenes query syntax. 
 * 
 * <h2 id="storedindexed">Fields are stored/indexed</h2>
 * All fields are stored and indexed. The "text" fields (gathering all literals)
 * have to be stored, because when a new literal is added to a document,
 * the previous texts need to be copied from  the existing document to the new Document,
 * this does not work when they are only "indexed".
 * Fields that are not stored, cannot be retrieved using full-text querying.
 * 
 * <h2>Deleting a Lucene index</h2>
 * At the moment, deleting the lucene index can be done in two ways:
 * <ul>
 *  <li>Delete the folder where the data is stored while the application is not running</li>
 *  <li>Call the repository's <code>{@link org.openrdf.repository.RepositoryConnection#clear(org.openrdf.model.Resource[])}</code> method with no arguments.
 *  <code>clear()</code>. This will delete the index.</li>
 * </ul>
 * 
 * <h2>Handling of Contexts</h2>
 * Each lucene document contains a field for every contextIDs that contributed
 * to the document.  
 * This means that when adding/appending to a document, all additional
 * context-uris are added to the document. When deleting individual triples,
 * the context is ignored. In clear(Resource ...) we make a query on all 
 * Lucene-Documents that were possibly created by this context(s). Given 
 * a document D that context C(1-n) contributed to. D' is the new document
 * after clear().
 * - if there is only one C then D can be safely removed. There is no D'
 *   (I hope this is the standard case: like in ontologies, where all triples
 *   about a resource are in one document)
 * - if there are multiple C, remember the uri of D, delete D, and query
 *   (s,p,o, ?) from the underlying store after committing the operation-
 *   this returns the literals of D', add D' as new document
 * This will probably be both fast in the common case and capable
 * enough in the multiple-C case.
 * 
 * <h2>Datatypes</h2>
 * Datatypes are ignored in the LuceneSail.
 */
public class LuceneSail extends NotifyingSailWrapper implements StackableSail {

   /*
	 * 
	 * FIXME: Add a proper reference to the ISWC paper in the Javadoc.
	 * Gunnar: only when/if the paper is accepted
	 * Enrico: paper was rejected
	 * Leo: We need to resubmit it. 
	 *
	 * 
	 * FIXME: Add settings that instruct a LuceneSailConnection or LuceneIndex
	 * which properties are to be handled in which way. This is conceptually
	 * similar to Lucene's Field types: should properties be stored in the
	 * wrapped Sail (enabling retrieval through RDF queries), indexed in the
	 * LuceneIndex (enabling full-text search using Lucene queries embedded in
	 * RDF graph queries) or both?
	 * 
	 * Gunnar and Leo: we had this in the old version, we might add later. 
	 * Enrico: in beagle we set the default setting to index AND store a field,
	 *  so that when you extend the ontology you can be sure it is indexed and
	 *  stored by the lucenesail without touching it. For certain (very rare) predicates
	 *  (like the full text of the resource) we then explicitly turned off the store
	 *  option. That would be a desired behaviour.
	 *  In the old version an RDF file was used, but it should be done differently,
	 *  that is too hard-coded! can't that information be stored in the wrapped sail itself?
	 *  Annotate a predicate with the proper lucene values (store / index / storeAndIndex),
	 *  if nothing is given, take the default, and read this on starting the lucenesail.
	 * Leo: ok, default = index and store, agreed.
    * Leo: about configuration: RDF config is agreed, if passed as file, inside the wrapped sail,
    * or in an extra sail should all be possible.
	 */
	
	/*
	 * 
	 * FIXME: This code can only handle RDF queries containing a single "Lucene
	 * expression" (i.e. a combination of matches, query and optionally other
	 * predicates from the LuceneSail's namespace), the other expressions are
	 * ignored. Extending this to support an arbitrary number of search
	 * expressions is theoretically possible but easier said then done,
	 * especially because of the number of different cases that need to be
	 * handled: variable subject vs. specified subject, expressions operating on
	 * the same subject vs. expressions operating on different subjects, etc.
	 * 
	 * Gunnar: I would we restrict this to one. Enrico might have other requirements? 
	 * Enrico: we need 1) an arbitrary number of lucene expressions and
	 *  2) an arbitrary combination with ordinary structured queries
	 *  (see lucenesail paper, fig. 1 on page 6)
     * Leo: combining lucene query with normal query is required, 
     * having multiple lucene queries in one SPARQL query is a good idea,
     * which should be doable. Lower priority.
	 * 
 * 
	 * FIXME: We should escape those chars in predicates/field names that have a
	 * special meaning in Lucene's query syntax, using ":" in a field name might
	 * lead to problems (it will when you start to query on these fields).
	 * Enrico: yes, we escaped those : sucessfully with a simple \, the only
	 * difficuilty was to figure out how many \ are needed (how often they get
	 * unescaped until they arrive at Lucene)
	 * 
	 * Leo noticed this. Gunnar asks: Does lucene not have a escape syntax? 
	 * 
	 * 
	 * 
	 * FIXME: The getScore method is a convenient and efficient way of testing
	 * whether a given document matches a query, as it adds the document URI to
	 * the Lucene query instead of firing the query and looping over the result
	 * set. The problem with this method is that I am not sure whether adding the
	 * URI to the Lucene query will lead to a different score for that document.
	 * For most applications this is probably not a problem as you either will
	 * use the search method with the scores reposted to its listener, or the
	 * getScore method, but not both. The order of matching documents will
	 * probably be the same when sorting on score (field is indexed without
	 * normalization + only unique values). Still, it is counterintuitive when a
	 * particular document is returned with a given score and a getScore for that
	 * same URI gives a different score.
	 * 
	 * FIXME: the code is very much NOT thread-safe, especially when you are
	 * changing the index and querying it with LuceneSailConnection at the same
	 * time: the IndexReaders/Searchers are closed after each statement addition
	 * or removal but they must also remain open while we are looping over search
	 * results. Also, internal document numbers are used in the communication
	 * between LuceneIndex and LuceneSailConnection, which is not a good idea.
	 * Some mechanism has to be introduced to support external querying while the
	 * index is being modified (basically: make sure that a single search process
	 * keeps using the same IndexSearcher).
	 * 
	 * Gunnar and Leo: we are not sure if the original lucenesail was 100% threadsafe, but 
	 * at least it had "synchronized" everywhere :)
	 *  
	 * http://gnowsis.opendfki.de/repos/gnowsis/trunk/lucenesail/src/java/org/openrdf/sesame/sailimpl/lucenesail/LuceneIndex.java 
	 * 
	 * This might be a big issue in Nepomuk...  
	 * Enrico: do we have multiple threads? do we need separate threads?
     * Leo: we have separate threads, but we don't care much for now.
	 * 
	 */

	final private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	/**
	 * Set the key "lucenedir=&lt;path&gt;" as sail parameter to configure the Lucene Directory on the 
	 * filesystem where to store the lucene index.
	 */
	public static final String LUCENE_DIR_KEY="lucenedir";
	

	/**
	 * Set the key "useramdir=true" as sail parameter to let the LuceneSail store its
	 * Lucene index in RAM. This is not intended for production environments.
	 */
	public static final String LUCENE_RAMDIR_KEY="useramdir";
	
	/**
	 * Set this key as sail parameter to configure the Lucene analyzer class
	 * implementation to use for text analysis.
	 */
	public static final String ANALYZER_CLASS_KEY="analyzer";
	/**
	 * Set this key as sail parameter to influence whether incomplete
	 * queries are treated as failure (Malformed queries) or whether 
	 * they are ignored. Set to either "true" or "false".
	 * When ommitted in the properties, true is default (failure on incomplete queries).
	 *
	 * see {@link #isIncompleteQueryFails()}
	 */
	public static final String INCOMPLETE_QUERY_FAIL_KEY="incompletequeryfail";
	
	/**
	 * The LuceneIndex holding the indexed literals.
	 */
	private LuceneIndex luceneIndex;
	private final Properties parameters=new Properties();
	private boolean incompleteQueryFails = true;

	public void setLuceneIndex(LuceneIndex luceneIndex) {
		this.luceneIndex = luceneIndex;
	}

	public LuceneIndex getLuceneIndex() {
		return luceneIndex;
	}

	@Override
	public NotifyingSailConnection getConnection() throws StoreException
	{
		return new LuceneSailConnection(super.getConnection(), luceneIndex, this);
	}

	@Override
	public void shutDown() throws StoreException
	{
		try {
			luceneIndex.shutDown();
		}
		catch (IOException e) {
			throw new StoreException(e);
		}
		finally {
			// ensure that super is also invoked when the LuceneIndex causes an
			// IOException
			super.shutDown();
		}
	}

	@Override
	public void initialize() throws StoreException
	{
		super.initialize();
		try { 
			if (parameters.containsKey(INCOMPLETE_QUERY_FAIL_KEY))
				setIncompleteQueryFails(Boolean.parseBoolean(parameters.getProperty(INCOMPLETE_QUERY_FAIL_KEY)));
			if (luceneIndex==null) {
				Analyzer analyzer; 
				if (parameters.containsKey(ANALYZER_CLASS_KEY)) {
					analyzer = (Analyzer)Class.forName((String)parameters.getProperty(ANALYZER_CLASS_KEY)).newInstance();
				} else { 
					analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
				}
				
				if (parameters.containsKey(LUCENE_DIR_KEY)) {
					FSDirectory dir = FSDirectory.open(new File(parameters.getProperty(LUCENE_DIR_KEY)));
					setLuceneIndex(new LuceneIndex(dir,analyzer));
				} else if (parameters.containsKey(LUCENE_RAMDIR_KEY) && "true".equals(parameters.getProperty(LUCENE_RAMDIR_KEY))) {
					RAMDirectory dir = new RAMDirectory();
					setLuceneIndex(new LuceneIndex(dir,analyzer));
				} 
				else { 
					throw new StoreException("No luceneIndex set, and no '"+LUCENE_DIR_KEY+"' or '"+LUCENE_RAMDIR_KEY+"' parameter given. ");
				}
			}
		} catch(Exception e) {
			throw new StoreException("Could not initialize LuceneSail: "+e.getMessage(),e);
		}
	}

	public void setParameter(String key, String value)
	{
		parameters.put(key,value);
	}

	
	/**
	 * When this is true, incomplete queries will trigger a SailException.
	 * You can set this value either using {@link #setIncompleteQueryFails(boolean)}
	 * or using the parameter "incompletequeryfail"
	 * @return Returns the incompleteQueryFails.
	 */
	public boolean isIncompleteQueryFails() {
		return incompleteQueryFails;
	}

	
	/**
	 * Set this to true, so that incomplete queries will trigger a SailException.
	 * Otherwise, incomplete queries will be logged with level WARN.
	 * Default is true.
	 * You can set this value also using the parameter "incompletequeryfail".
	 * @param incompleteQueryFails true or false
	 */
	public void setIncompleteQueryFails(boolean incompleteQueryFails) {
		this.incompleteQueryFails = incompleteQueryFails;
	}
	
	/**
	 * Starts a reindexation process of the whole sail.
	 * Basically, this will delete and add all data again, a long-lasting
	 * process.
	 * @throws IOException 
	 */
	public void reindex() throws Exception {
		// clear
		logger.info("Reindexing sail: clearing...");
		luceneIndex.clear();
		logger.info("Reindexing sail: adding...");
		// TODO: this is BAD BAD BAD and a hack because result ordering is not implemented in sesame
		// START HACK
		SailConnection con = getDelegate().getConnection();
		try {
			LinkedList<Statement> buffer = new LinkedList<Statement>();
			LinkedList<Statement> removed = new LinkedList<Statement>();
			// we do not reindex inferred triples (or?)
			Cursor<? extends Statement> it = con.getStatements(null, null, null, false);
			Statement s = it.next();
			while (s != null) {
				buffer.add(s);
				s = it.next();
			}
			// now the bad news
			logger.info("Reindexing sail: adding "+buffer.size()+" statements into the index in one operation. This is a bad hack and should be changed once SPARQL result ordering works.");
			luceneIndex.addRemoveStatements(buffer, removed, getDelegate());
		} finally {
			con.close();
		}
		// END OF HACK
		
		// TODO: comment this in once result ordering IS IMPLEMENTED
		/*
		// iterate
		SailRepository repo = new SailRepository(getBaseSail());
		//repo.initialize(); we don't need to initialize, that should be done already by others
		SailRepositoryConnection connection = repo.getConnection();
		try {
			TupleQuery query = connection.prepareTupleQuery(QueryLanguage.SPARQL, 
					"SELECT ?s ?p ?o ?c WHERE {GRAPH ?c {?s ?p ?o.}} ORDER BY ?s");
			TupleQueryResult res = query.evaluate();
			Resource current = null;
			LinkedList<Statement> statements = new LinkedList<Statement>();
			while (res.hasNext()) {
				BindingSet set = res.next();
				Resource r = (Resource)set.getValue("s");
				URI p = (URI)set.getValue("p");
				Value o = set.getValue("o");
				Resource c = (Resource)set.getValue("c");
				if (current == null)
				{
					current = r;
				} else if (!current.equals(r)) {
					if (logger.isDebugEnabled())
						logger.debug("reindexing resource "+current);
					// commit
					luceneIndex.addDocument(current, statements);
					
					// re-init
					current = r;
					statements.clear();
				}
				statements.add(new ContextStatementImpl(r, p, o, c));
			}
			
		} finally {
			connection.close();
		}
		*/
		logger.info("Reindexing sail: done.");
	}
}


