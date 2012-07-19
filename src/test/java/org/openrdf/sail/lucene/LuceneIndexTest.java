/*
 * Copyright Aduna, DFKI and L3S (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.memory.MemoryStore;

public class LuceneIndexTest extends TestCase {
	
	
	public static final URI CONTEXT_1 = new URIImpl("urn:context1");

	public static final URI CONTEXT_2 = new URIImpl("urn:context2");

	public static final URI CONTEXT_3 = new URIImpl("urn:context3");
	
	// create some objects that we will use throughout this test
	URI subject = new URIImpl("urn:subj");
	URI subject2 = new URIImpl("urn:subj2");
	URI predicate1 = new URIImpl("urn:pred1");
	URI predicate2 = new URIImpl("urn:pred2");
	Literal object1 = new LiteralImpl("object1");
	Literal object2 = new LiteralImpl("object2");
	Literal object3 = new LiteralImpl("cats");
	Literal object4 = new LiteralImpl("dogs");
	Literal object5 = new LiteralImpl("chicken");
	Statement statement11 = new StatementImpl(subject, predicate1, object1);
	Statement statement12 = new StatementImpl(subject, predicate2, object2);
	Statement statement21 = new StatementImpl(subject2, predicate1, object3);
	Statement statement22 = new StatementImpl(subject2, predicate2, object4);
	Statement statement23 = new StatementImpl(subject2, predicate2, object5);
	Statement statementContext111 = new StatementImpl(subject, predicate1, object1, CONTEXT_1);
	Statement statementContext121 = new StatementImpl(subject, predicate2, object2, CONTEXT_1);
	Statement statementContext211 = new StatementImpl(subject2, predicate1, object3, CONTEXT_1);
	Statement statementContext222 = new StatementImpl(subject2, predicate2, object4, CONTEXT_2);
	Statement statementContext232 = new StatementImpl(subject2, predicate2, object5, CONTEXT_2);

	// add a statement to an index
	RAMDirectory directory;
	StandardAnalyzer analyzer;
	LuceneIndex index;

	@Override
	protected void setUp()
		throws Exception
	{
		directory = new RAMDirectory();
		analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		index = new LuceneIndex(directory, analyzer);
	}

	@Override
	protected void tearDown()
		throws Exception
	{
		index.shutDown();
		super.tearDown();
	}

	public void testAddStatement()
		throws IOException, ParseException
	{
		// add a statement to an index
		index.addStatement(statement11);
		index.commit();

		// check that it arrived properly
		IndexReader reader = IndexReader.open(directory, false);
		assertEquals(1, reader.numDocs());

		Term term = new Term(LuceneIndex.ID_FIELD_NAME, subject.toString());
		TermDocs docs = reader.termDocs(term);
		assertTrue(docs.next());

		int documentNr = docs.doc();
		Document document = reader.document(documentNr);
		assertEquals(subject.toString(), document.get(LuceneIndex.ID_FIELD_NAME));
		assertEquals(object1.getLabel(), document.get(predicate1.toString()));

		assertFalse(docs.next());
		docs.close();
		reader.close();

		// add another statement
		index.addStatement(statement12);

		// See if everything remains consistent. We must create a new IndexReader
		// in order to be able to see the updates
		reader = IndexReader.open(directory, false);
		assertEquals(1, reader.numDocs()); // #docs should *not* have increased

		docs = reader.termDocs(term);
		assertTrue(docs.next());

		documentNr = docs.doc();
		document = reader.document(documentNr);
		assertEquals(subject.toString(), document.get(LuceneIndex.ID_FIELD_NAME));
		assertEquals(object1.getLabel(), document.get(predicate1.toString()));
		assertEquals(object2.getLabel(), document.get(predicate2.toString()));

		assertFalse(docs.next());
		docs.close();

		// see if we can query for these literals
		IndexSearcher searcher = new IndexSearcher(reader);
		QueryParser parser = new QueryParser(Version.LUCENE_CURRENT, LuceneIndex.TEXT_FIELD_NAME, analyzer);

		Query query = parser.parse(object1.getLabel());
		System.out.println("query=" + query);
		TopDocs hits = searcher.search(query, 2);
		assertEquals(1, hits.totalHits);

		query = parser.parse(object2.getLabel());
		hits = searcher.search(query,2);
		assertEquals(1, hits.totalHits);

		searcher.close();
		reader.close();
		
		// remove the first statement
		index.removeStatement(statement11);
		
		// check that that statement is actually removed and that the other still
		// exists
		reader = IndexReader.open(directory, false);
		assertEquals(1, reader.numDocs());

		docs = reader.termDocs(term);
		assertTrue(docs.next());

		documentNr = docs.doc();
		document = reader.document(documentNr);
		assertEquals(subject.toString(), document.get(LuceneIndex.ID_FIELD_NAME));
		assertNull(document.get(predicate1.toString()));
		assertEquals(object2.getLabel(), document.get(predicate2.toString()));

		assertFalse(docs.next());
		docs.close();
		
		reader.close();
		
		// remove the other statement
		index.removeStatement(statement12);

		// check that there are no documents left (i.e. the last Document was
		// removed completely, rather than its remaining triple removed)
		reader = IndexReader.open(directory, false);
		assertEquals(0, reader.numDocs());
		reader.close();
	}
	
	public void testAddMultiple() throws Exception{
		// add a sail
		MemoryStore memoryStore = new MemoryStore();
		// enable lock tracking
		info.aduna.concurrent.locks.Properties.setLockTrackingEnabled(true);
		LuceneSail sail = new LuceneSail();
		sail.setDelegate(memoryStore);
		sail.setLuceneIndex(index);

		// create a Repository wrapping the LuceneSail
		SailRepository repository = new SailRepository(sail);
		repository.initialize();
		
		// now add the statements through the repo
		// add statements with context
		SailRepositoryConnection connection = repository.getConnection();
		connection.begin();
		
		// add a statement to an index
		connection.add(statement11);
		connection.add(statement12);
		connection.add(statement21);
		connection.add(statement22);
		connection.commit();

		// check that it arrived properly
		IndexReader reader = IndexReader.open(directory, false);
		assertEquals(2, reader.numDocs());
		reader.close();
		
		// check the documents
		Document document = index.getDocument(subject);
		assertEquals(subject.toString(), document.get(LuceneIndex.ID_FIELD_NAME));
		assertStatement(statement11, document);
		assertStatement(statement12, document);
		
		document = index.getDocument(subject2);
		assertEquals(subject2.toString(), document.get(LuceneIndex.ID_FIELD_NAME));
		assertStatement(statement21, document);
		assertStatement(statement22, document);

		// add/remove one
//		added.clear();
//		removed.clear();
		connection.begin();
		connection.add(statement23);
		connection.remove(statement22);
		connection.commit();
		
//		added.add(statement23);
//		removed.add(statement22);
//		index.addRemoveStatements(added, removed);
		
		// check doc 2
		document = index.getDocument(subject2);
		assertEquals(subject2.toString(), document.get(LuceneIndex.ID_FIELD_NAME));
		assertStatement(statement21, document);
		assertStatement(statement23, document);
		assertNoStatement(statement22, document);
		
		// TODO: check deletion of the rest
		
	}
	
	/**
	 * Contexts can only be tested in combination with a sail, 
	 * as the triples have to be retrieved from the sail
	 * @throws Exception
	 */
	public void testContexts()throws Exception {
		// add a sail
		MemoryStore memoryStore = new MemoryStore();
		// enable lock tracking
		info.aduna.concurrent.locks.Properties.setLockTrackingEnabled(true);
		LuceneSail sail = new LuceneSail();
		sail.setDelegate(memoryStore);
		sail.setLuceneIndex(index);

		// create a Repository wrapping the LuceneSail
		SailRepository repository = new SailRepository(sail);
		repository.initialize();
		
		// now add the statements through the repo
		// add statements with context
		SailRepositoryConnection connection = repository.getConnection();
		try {
//			connection.setAutoCommit(false);
			connection.begin();
			connection.add(statementContext111, statementContext111.getContext());
			connection.add(statementContext121, statementContext121.getContext());
			connection.add(statementContext211, statementContext211.getContext());
			connection.add(statementContext222, statementContext222.getContext());
			connection.add(statementContext232, statementContext232.getContext());
			connection.commit();
				
			// check if they are there
			assertStatement(statementContext111);
			assertStatement(statementContext121);
			assertStatement(statementContext211);
			assertStatement(statementContext222);
			assertStatement(statementContext232);
	
			// delete context 1
			connection.begin();
			connection.clear(new Resource[]{CONTEXT_1});
			connection.commit();
			assertNoStatement(statementContext111);
			assertNoStatement(statementContext121);
			assertNoStatement(statementContext211);
			assertStatement(statementContext222);
			assertStatement(statementContext232);
		} finally {
			// close repo
			connection.close();
			repository.shutDown();
		}
	}
	
	/**
	 * Contexts can only be tested in combination with a sail, 
	 * as the triples have to be retrieved from the sail
	 * @throws Exception
	 */
	public void testContextsRemoveContext2()throws Exception {
		// add a sail
		MemoryStore memoryStore = new MemoryStore();
		// enable lock tracking
		info.aduna.concurrent.locks.Properties.setLockTrackingEnabled(true);
		LuceneSail sail = new LuceneSail();
		sail.setDelegate(memoryStore);
		sail.setLuceneIndex(index);
		
		// create a Repository wrapping the LuceneSail
		SailRepository repository = new SailRepository(sail);
		repository.initialize();
		
		// now add the statements through the repo
		// add statements with context
		SailRepositoryConnection connection = repository.getConnection();
		try {
//			connection.setAutoCommit(false);
			connection.begin();
			connection.add(statementContext111, statementContext111.getContext());
			connection.add(statementContext121, statementContext121.getContext());
			connection.add(statementContext211, statementContext211.getContext());
			connection.add(statementContext222, statementContext222.getContext());
			connection.add(statementContext232, statementContext232.getContext());
			connection.commit();
			
			// check if they are there
			assertStatement(statementContext111);
			assertStatement(statementContext121);
			assertStatement(statementContext211);
			assertStatement(statementContext222);
			assertStatement(statementContext232);
			
			// delete context 2
			connection.begin();
			connection.clear(new Resource[]{CONTEXT_2});
			connection.commit();
			assertStatement(statementContext111);
			assertStatement(statementContext121);
			assertStatement(statementContext211);
			assertNoStatement(statementContext222);
			assertNoStatement(statementContext232);
		} finally {
			// close repo
			connection.close();
			repository.shutDown();
		}
	}
	
	private void assertStatement(Statement statement) throws Exception {
		Document document = index.getDocument(statement.getSubject());
		if (document == null)
			fail("Missing document "+statement.getSubject());
		assertStatement(statement, document);
	}
	
	private void assertNoStatement(Statement statement) throws Exception {
		Document document = index.getDocument(statement.getSubject());
		if (document == null)
			return;
		assertNoStatement(statement, document);
	}

	/**
	 * @param statement112
	 * @param document
	 */
	private void assertStatement(Statement statement, Document document) {
		Field[] fields = document.getFields(statement.getPredicate().toString());
		assertNotNull("field "+statement.getPredicate()+" not found in document "+document, fields);
		for (Field f:fields)
		{
			if (((Literal)statement.getObject()).getLabel().equals(f.stringValue()))
					return;
		}
		fail("Statement not found in document "+statement);
	}
	
	/**
	 * @param statement112
	 * @param document
	 */
	private void assertNoStatement(Statement statement, Document document) {
		Field[] fields = document.getFields(statement.getPredicate().toString());
		if (fields == null)
			return;
		for (Field f:fields)
		{
			if (((Literal)statement.getObject()).getLabel().equals(f.stringValue()))
				fail("Statement should not be found in document "+statement);
		}
		
	}
}
