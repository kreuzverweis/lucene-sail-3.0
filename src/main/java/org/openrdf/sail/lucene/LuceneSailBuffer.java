/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 2007.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.sail.lucene;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;


/**
 * A buffer collecting all transaction operations (triples that need to be added,
 * removed, clear operations) so that they can be executed at once during commit.
 * 
 * @author sauermann
 */
public class LuceneSailBuffer {
	
	/*
	public class Statement extends StatementImpl {
		Resource[] contexts;

		public Statement(Resource subject, URI predicate, Value object, Resource[] contexts) {
			super(subject, predicate, object);
			this.contexts = contexts;
		}

		@Override
		public boolean equals(Object obj)
		{
			boolean b = super.equals(obj);
			if (b)
			{
				// contexts equal?
				return contexts.equals(((Statement)obj).contexts);
			}
			else return false;
		}

		@Override
		public int hashCode()
		{
			int result = 0;
			for (Resource resource : contexts)
				result += resource.hashCode();
			result *= 7919;
			result += super.hashCode();
			return result;
		}
	}
	*/

	public class Operation {
		
	}
	
	public class AddRemoveOperation extends Operation {
	
		HashSet<Statement> added = new HashSet<Statement>();
		HashSet<Statement> removed = new HashSet<Statement>();
		public void add(Statement s) {
			added.add(s);
			removed.remove(s);
		}
		
		public void remove(Statement s) {
			removed.add(s);
			added.remove(s);
		}

		/**
		 * @return Returns the added.
		 */
		public HashSet<Statement> getAdded() {
			return added;
		}

		
		/**
		 * @return Returns the removed.
		 */
		public HashSet<Statement> getRemoved() {
			return removed;
		}
		
	}
	
	public class ClearContextOperation extends Operation {
		Resource[] contexts;
		public ClearContextOperation(Resource[] contexts) {
			this.contexts = contexts;
		}
		
		/**
		 * @return Returns the contexts.
		 */
		public Resource[] getContexts() {
			return contexts;
		}
		
	}
	
	public class ClearOperation extends Operation {
		
	}
	
	private ArrayList<Operation> operations = new ArrayList<Operation>();
	
	/**
	 * Add this statement to the buffer
	 * @param s the statement
	 */
	public synchronized void add(Statement s) {
		// check if the last operation was adding/Removing triples
		Operation o = (operations.isEmpty()) ? null : operations.get(operations.size()-1);
		if ((o == null) || !(o instanceof AddRemoveOperation))
		{
			o = new AddRemoveOperation();
			operations.add(o);
		}
		AddRemoveOperation aro = (AddRemoveOperation)o;
		aro.add(s);
	}
	
	/**
	 * Remove this statement to the buffer
	 * @param s the statement
	 */
	public synchronized void remove(Statement s) {
		// check if the last operation was adding/Removing triples
		Operation o = (operations.isEmpty()) ? null : operations.get(operations.size()-1);
		if ((o == null) || !(o instanceof AddRemoveOperation))
		{
			o = new AddRemoveOperation();
			operations.add(o);
		}
		AddRemoveOperation aro = (AddRemoveOperation)o;
		aro.remove(s);
	}
	
	public synchronized void clear(Resource[] contexts) {
		if ((contexts == null) || (contexts.length == 0))
			operations.add(new ClearOperation());
		else
			operations.add(new ClearContextOperation(contexts));
	}

	
	/**
	 * Iterator over the operations
	 * @return
	 */
	public synchronized Iterator<Operation> operationsIterator() {
		return operations.iterator();
	}
	
	/**
	 * the list of operations. You must not change it
	 * @return
	 */
	public synchronized List<Operation> operations() {
		return operations;
	}

	/**
	 * Optimize will remove any changes that are done before a 
	 * clear()
	 */
	public void optimize() {
		for (int i = operations.size()-1; i>=0; i--) {
			Operation o = operations.get(i);
			if (o instanceof ClearOperation) {
				// remove everything before
				// is is now the size of the operations to be removed
				while (i>0)
				{
					operations.remove(i);
					i--;
				}
				return;
			}
		}
	}

	/**
	 * reset the buffer, empty the operations list
	 */
	public void reset() {
		operations.clear();
	}
	


}
