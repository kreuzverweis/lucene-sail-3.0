/*
 * Copyright Aduna, DFKI and L3S (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.openrdf.cursor.Cursor;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.QueryModel;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.sail.helpers.NotifyingSailConnectionWrapper;
import org.openrdf.sail.lucene.LuceneSailBuffer.AddRemoveOperation;
import org.openrdf.sail.lucene.LuceneSailBuffer.ClearContextOperation;
import org.openrdf.sail.lucene.LuceneSailBuffer.Operation;
import org.openrdf.store.StoreException;

/**
 * 
 * <h2><a name="whySailConnectionListener">Sail Connection Listener instead of implementing add/remove</a></h2>
 * Using SailConnectionListener, see <a href="#whySailConnectionListener">above</a>
 * The LuceneIndex is adapted based on events coming from the wrapped
 * Sail, rather than by overriding the addStatement and removeStatements
 * methods. This approach has two benefits: (1) when the wrapped Sail only
 * reports statements that were not stored before, the LuceneIndex does
 * not have to do the check on the skipped statemements and (2) the method
 * for removing Statements from the Lucene index does not have to take
 * wildcards into account, making its implementation simpler.
 * 
 * <h2>Synchronized Methods</h2>
 * LuceneSailConnection uses a listener to collect removed statements.
 * The listener should not be active during the removal of contexts, as this is not 
 * needed (context removal is implemented differently). To realize this,
 * all methods that can do changes are synchronized and during context removal,
 * the listener is disabled. Thus, all methods of this connection
 * that can change data are synchronized.
 * 
 * <h2>Evaluating Queries - possible optimizations</h2>
 * Arjohn has answered this question in the sesame-dev mailinglist on 13.8.2007:
 * <b>Is there a QueryModelNode that can contain a fixed (perhaps very long)
 * list of Query result bindings?</b>
 * There is currently no such node, but there are two options to get
similar behaviour:

1) Include the result bindings as OR-ed constraints in the query model.
    E.g. if you have a result binding like {{x=1,y=1},{x=2,y=2}}, this
    translates to the constraints (x=1 and y=1) or (x=2 and y=2).

2) The LuceneSail could iterate over the LuceneQueryResult and supply
    the various results as query input parameters to the underlying Sail.
    This is similar to using PreparedStatement's in JDBC.
 
 
 *
 * @author sauermann
 */
public class LuceneSailConnection extends NotifyingSailConnectionWrapper {

	final private Logger logger = LoggerFactory.getLogger(this.getClass());

	final private LuceneIndex luceneIndex;
	
	final private LuceneSail sail;

	/**
	 * the buffer that collects operations
	 * TODO: handle begin
	 */
	final private LuceneSailBuffer buffer = new LuceneSailBuffer();
	
	/**
	 * The listener that listens to the underlying connection.
	 * It is disabled during clearContext operations.
	 */
	protected final SailConnectionListener connectionListener = new SailConnectionListener() {

		public void statementAdded(Statement statement) {
			// we only consider statements that contain literals
			if(statement.getObject() instanceof Literal)
				buffer.add(statement);
			//LuceneSailConnection.this.luceneIndex.addStatement(statement);
		}

		public void statementRemoved(Statement statement) {
			// we only consider statements that contain literals
			if(statement.getObject() instanceof Literal)
				buffer.remove(statement);
			//LuceneSailConnection.this.luceneIndex.removeStatement(statement);
		}
	};

	public LuceneSailConnection(NotifyingSailConnection wrappedConnection, LuceneIndex luceneIndex, LuceneSail sail) {
		super(wrappedConnection);
		this.luceneIndex = luceneIndex;
		this.sail = sail;

		/*
		 * Using SailConnectionListener, see <a href="#whySailConnectionListener">above</a>

		 */

		if (wrappedConnection instanceof NotifyingSailConnection)
			((NotifyingSailConnection)wrappedConnection).addConnectionListener(connectionListener);
	}

	@Override
	public synchronized void addStatement(Resource arg0, URI arg1, Value arg2, Resource... arg3) throws StoreException
	{
		super.addStatement(arg0, arg1, arg2, arg3);
	}

	// //////////////////////////////// Methods related to indexing

// @TODO can we remove this?	
	
//	@Override
//	public synchronized void clear(Resource... arg0)
//		throws StoreException
//	{
//		// remove the connection listener, this is safe as the changing methods are synchronized
//		// during the clear(), no other operation can be invoked
//		if (getDelegate() instanceof NotifyingSailConnection)
//			((NotifyingSailConnection)getDelegate()).removeConnectionListener(connectionListener);
//
//		//getWrappedConnection().removeConnectionListener(connectionListener);
//		try {
//			super.clear(arg0);
//			buffer.clear(arg0);
//		} finally {
//			if (getDelegate() instanceof NotifyingSailConnection)
//				((NotifyingSailConnection)getDelegate()).addConnectionListener(connectionListener);
//		}
//	}

	@Override
	public void commit() throws StoreException
	{
		super.commit();

		logger.debug("Committing Lucene transaction with "+buffer.operations().size()+" operations.");
		try {
			try {
				// preprocess buffer
				buffer.optimize();
				
				// run operations and remove them from buffer
				for (Iterator<Operation> i = buffer.operations().iterator(); i.hasNext(); )
				{
					Operation op = i.next();
					if (op instanceof LuceneSailBuffer.AddRemoveOperation) {
						AddRemoveOperation addremove = (AddRemoveOperation)op;
						// add/remove in one call
						logger.debug("indexing "+addremove.getAdded().size()+"/removing " +addremove.getRemoved().size()+ " statements...");
						luceneIndex.addRemoveStatements(addremove.getAdded(), addremove.getRemoved(), sail.getDelegate());
					} else if (op instanceof LuceneSailBuffer.ClearContextOperation) {
						// clear context
						logger.debug("clearing contexts...");
						luceneIndex.clearContexts(((ClearContextOperation)op).getContexts(), sail.getDelegate());
					} else if (op instanceof LuceneSailBuffer.ClearOperation) {
						logger.debug("clearing index...");
						luceneIndex.clear();
					} else  throw new RuntimeException("Cannot interpret operation "+op+" of type "+op.getClass().getName());
					i.remove();
				}
				luceneIndex.commit();
			}
			catch (Exception e) {
				logger.error("Committing operations in lucenesail, encountered exception "+e+". Only some operations were stored, "+buffer.operations().size()+" operations are discarded. Lucene Index is now corrupt.", e);
				throw new StoreException(e);
			}
		} finally {
			buffer.reset();
		}
	}

	// //////////////////////////////// Methods related to querying

	@Override
	public Cursor<? extends BindingSet> evaluate(QueryModel query, BindingSet bindings, boolean includeInferred)
		throws StoreException
	{
		// lookup the Lucene queries in this TupleExpr
		QuerySpecBuilder interpreter = new QuerySpecBuilder(sail.isIncompleteQueryFails());
		Set<QuerySpec> queries = interpreter.process(query, bindings);
		int nrQueries = queries.size();

		// if the query does not contain any lucene queries, let the lower sail evaluate the query
		if (nrQueries == 0) {
			return super.evaluate(query, bindings, includeInferred);
		}
	// its our turn to evaluate the query
     	return WrappedLuceneQueryIteratorFactory.getIterator(
     			luceneIndex, 
     			this.getDelegate(), 
     			this.getValueFactory(),
     			queries, 
     			query, 
     			bindings, 
     			includeInferred);	
   }
	

	@Override
	public synchronized void removeStatements(Resource arg0, URI arg1, Value arg2, Resource... arg3) throws StoreException
	{
		/*
		 * 
		 */
		super.removeStatements(arg0, arg1, arg2, arg3);
	}

	@Override
	public void rollback() throws StoreException
	{
		super.rollback();
		buffer.reset();
		try {
			luceneIndex.rollback();
		}
		catch (IOException e) {
			throw new StoreException(e);
		}
	}
}
