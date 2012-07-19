package org.openrdf.sail.lucene;

import java.util.Set;

import org.openrdf.cursor.Cursor;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.Distinct;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModel;
import org.openrdf.query.algebra.Reduced;
import org.openrdf.query.algebra.Slice;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.cursors.DistinctCursor;
import org.openrdf.query.algebra.evaluation.cursors.LimitCursor;
import org.openrdf.query.algebra.evaluation.cursors.MultiProjectionCursor;
import org.openrdf.query.algebra.evaluation.cursors.OffsetCursor;
import org.openrdf.query.algebra.evaluation.cursors.OrderCursor;
import org.openrdf.query.algebra.evaluation.cursors.ProjectionCursor;
import org.openrdf.query.algebra.evaluation.cursors.ReducedCursor;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.util.OrderComparator;
import org.openrdf.query.algebra.evaluation.util.ValueComparator;
import org.openrdf.sail.SailConnection;
import org.openrdf.store.StoreException;


/**
 *
 * @author sschenk
 */
public class WrappedLuceneQueryIteratorFactory {

	public static Cursor<BindingSet> getIterator(
			LuceneIndex index, 
			SailConnection sailConn, 
			ValueFactory vf,
			Set<QuerySpec> queries, 
			QueryModel query, 
			BindingSet bindings, 
			boolean includeInferred) throws StoreException {
		if (query.getTupleExpr() instanceof MultiProjection) {
			MultiProjection exp = (MultiProjection)query.getTupleExpr();
			return new MultiProjectionCursor(exp, 
					getIterator(index, sailConn, vf, queries, 
							new QueryModel(exp.getArg(), query.getDefaultGraphs(), query.getNamedGraphs()), 
							bindings, includeInferred), 
					bindings);
		} else if (query.getTupleExpr() instanceof Projection) {
			Projection exp = (Projection)query.getTupleExpr();
			return new ProjectionCursor(exp, 
					getIterator(index, sailConn, vf, queries, 
							new QueryModel(exp.getArg(), query.getDefaultGraphs(), query.getNamedGraphs()), 
							bindings, includeInferred), 
					bindings);
		} else if (query.getTupleExpr() instanceof Slice) {
			Slice exp = (Slice)query.getTupleExpr();
			return new LimitCursor<BindingSet>(
					new OffsetCursor<BindingSet>(
							getIterator(index, sailConn, vf, queries, 
									new QueryModel(exp.getArg(), query.getDefaultGraphs(), query.getNamedGraphs()), 
									bindings, includeInferred), 
							exp.getOffset()), exp.getLimit());
		} else if (query.getTupleExpr() instanceof Distinct) {
			Distinct exp = (Distinct)query.getTupleExpr();
			return new DistinctCursor<BindingSet>(
					getIterator(index, sailConn, vf, queries, 
							new QueryModel(exp.getArg(), query.getDefaultGraphs(), query.getNamedGraphs()), 
							bindings, includeInferred));
		} else if (query.getTupleExpr() instanceof Reduced) {
			Reduced exp = (Reduced)query.getTupleExpr();
			return new ReducedCursor<BindingSet>(
					getIterator(index, sailConn, vf, queries, 
							new QueryModel(exp.getArg(), query.getDefaultGraphs(), query.getNamedGraphs()), 
							bindings, includeInferred));
		} else if (query.getTupleExpr() instanceof Order) {
			Order exp = (Order)query.getTupleExpr();
			ValueComparator vcmp = new ValueComparator();
			OrderComparator cmp = new OrderComparator(
					new EvaluationStrategyImpl(new DummyTripleSource(vf)), 
					exp, vcmp);
			return new OrderCursor(
					getIterator(index, sailConn, vf, queries, 
							new QueryModel(exp.getArg(), query.getDefaultGraphs(), query.getNamedGraphs()), 
							bindings, includeInferred), 
					cmp);				
		} else {
			return new LuceneQueryIterator(index, sailConn, queries, query, bindings, includeInferred);
		}		
	}

	private static class DummyTripleSource implements TripleSource {

		private ValueFactory vf;

		public DummyTripleSource(ValueFactory vf) {
			this.vf = vf;
		}

		public Cursor<? extends Statement> getStatements(
				Resource arg0, URI arg1, Value arg2, Resource... arg3)
				{
			return null;
				}

		public ValueFactory getValueFactory() {
			return vf;
		}
	}

}
