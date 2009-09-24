package meetup.beeno;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import meetup.beeno.filter.ColumnRowFilter;
import meetup.beeno.filter.WhileMatchFilter;
import meetup.beeno.util.IOUtil;
import meetup.beeno.util.PBUtil;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * Utility for building up criteria for HBaseEntity queries.
 * 
 * @author garyh
 *
 */
public class Criteria implements Externalizable {
	
	private static Logger log = Logger.getLogger(Criteria.class);
	private List<Expression> expressions = new ArrayList<Expression>();

	public Criteria() {
	}

	public Criteria add(Expression expr) {
		this.expressions.add(expr);
		return this;
	}
	
	public List<Expression> getExpressions() {
		return this.expressions;
	}
	
	@Override
	public void readExternal( ObjectInput in ) throws IOException,
			ClassNotFoundException {
		this.expressions = (in.readBoolean() ? null : (List<Expression>)in.readObject());
	}

	@Override
	public void writeExternal( ObjectOutput out ) throws IOException {
		IOUtil.writeNullable(out, this.expressions);
	}

	
	
	/* ************* Expressions and builder methods ************** */
	
	public static Expression require(Expression expr) {
		return new RequireExpression(expr);
	}

	public static Expression and(Expression... expr) {
		CompoundExpression wrapper = new CompoundExpression(true);
		for (Expression e : expr) {
			wrapper.add(e);
		}

		return wrapper;
	}
	
	public static Expression or(Expression... expr) {
		CompoundExpression wrapper = new CompoundExpression(false);
		for (Expression e : expr) {
			if (log.isDebugEnabled())
				log.debug(String.format("Adding OR expression %s", expr.toString()));
			wrapper.add(e);
		}
		
		return wrapper;
	}
	
	public static Expression or(List<Expression> expr) {
		CompoundExpression wrapper = new CompoundExpression(false);
		for (Expression e : expr) {
			if (log.isDebugEnabled())
				log.debug(String.format("Adding OR expression %s", expr));
			wrapper.add(e);
		}
		
		return wrapper;		
	}
	
	public static Expression eq(String prop, Object val) {
		return new PropertyComparison(prop, val, ColumnRowFilter.CompareOp.EQUAL);
	}

	public static Expression ne(String prop, Object val) {
		return new PropertyComparison(prop, val, ColumnRowFilter.CompareOp.NOT_EQUAL);
	}

	public static abstract class Expression implements Externalizable {		
		public Expression() {
		}
		
		public abstract Filter getFilter(EntityMetadata.EntityInfo info) throws HBaseException;
		
		public String toString() {
			return "["+this.getClass().getSimpleName()+"]";
		}
	}
	
	
	public static abstract class PropertyExpression extends Expression implements Externalizable {
		protected String property;
		protected Object value;
		
		public PropertyExpression() {
			// for Externalizable
		}
		
		public PropertyExpression(String prop, Object val) {
			this.property = prop;
			this.value = val;
		}
		
		public String getProperty() { return this.property; }
		public Object getValue() { return this.value; }
		
		@Override
		public void readExternal( ObjectInput in ) throws IOException,
				ClassNotFoundException {
			this.property = IOUtil.readString(in);
			this.value = IOUtil.readWithType(in);
		}

		@Override
		public void writeExternal( ObjectOutput out ) throws IOException {
			IOUtil.writeNullable(out, this.property);
			IOUtil.writeNullableWithType(out, this.value);			
		}

		public String toString() {
			return "["+this.getClass().getSimpleName()+": property="+this.property+", value="+this.value+"]";
		}
	}
	
	public static class PropertyComparison extends PropertyExpression {
		
		private ColumnRowFilter.CompareOp op = null;
		
		public PropertyComparison() {
			// for Externalizable
		}
		
		public PropertyComparison(String prop, Object val, ColumnRowFilter.CompareOp op) {
			super(prop, val);
			this.op = op;
		}
		
		public Filter getFilter(EntityMetadata.EntityInfo entityInfo) throws HBaseException {
			EntityMetadata.FieldMapping mapping = entityInfo.getPropertyMapping(this.property);
			if (mapping == null) {
				throw new MappingException( entityInfo.getEntityClass(),
											String.format("No mapping for criteria!  class=%s, property=%s", 
													entityInfo.getEntityClass().getName(), this.property) );
			}
			
			if (log.isDebugEnabled()) {
				log.debug(String.format("PropertyComparison(%s, %s, %s): Creating ColumnRowFilter, column=%s", 
						  this.property, this.value, this.op.toString(), mapping.getFieldName()));
			}
			return new ColumnRowFilter(Bytes.toBytes(mapping.getFieldName()), 
										 this.op,
										 PBUtil.toBytes(this.value),
										 true);
		}
		
		public String toString() {
			return String.format("[%s: property=%s, value=%s]", this.getClass().getSimpleName(), this.property, this.value.toString());
		}
	}
	
	public static class RequireExpression extends Expression {
		private Expression required;
		public RequireExpression() {
			// for Externalizable
		}
		
		public RequireExpression(Expression required) {
			this.required = required;
		}
		
		public Filter getFilter(EntityMetadata.EntityInfo entityInfo) throws HBaseException {
			if (log.isDebugEnabled())
				log.debug(String.format("Adding filter: WhileMatchFilter for expr %s", this.required.toString()));
			
			Filter newFilter = new WhileMatchFilter(required.getFilter(entityInfo));
			return newFilter;
		}
		
		public Expression getRequired() {
			return this.required;
		}
		
		@Override
		public void readExternal( ObjectInput in ) throws IOException,
				ClassNotFoundException {
			this.required = (in.readBoolean() ? null : (Expression)in.readObject());
		}
		
		@Override
		public void writeExternal( ObjectOutput out ) throws IOException {
			IOUtil.writeNullable(out, this.required);
		}
		
		public String toString() {
			return String.format("[%s: required=%s]", this.getClass().getSimpleName(), this.required.toString());
		}
	}
	
	
	public static class CompoundExpression extends Expression {
		private FilterList.Operator oper = null;
		private List<Expression> subconditions = new ArrayList<Expression>();
		public CompoundExpression() {
			// for Externalizable
		}
		
		public CompoundExpression(boolean reqAll) {
			if (reqAll) 
				this.oper = FilterList.Operator.MUST_PASS_ALL;
			else
				this.oper = FilterList.Operator.MUST_PASS_ONE;
		}
		
		public Filter getFilter(EntityMetadata.EntityInfo entityInfo) throws HBaseException {
			FilterList newFilter = new FilterList(this.oper, new ArrayList<Filter>());
			for (Expression expr : this.subconditions) {
				newFilter.addFilter(expr.getFilter(entityInfo));
			}

			return newFilter;
		}
		
		public void add(Expression e) { this.subconditions.add(e); }
		
		@Override
		public void readExternal( ObjectInput in ) throws IOException,
				ClassNotFoundException {
			this.oper = (FilterList.Operator)IOUtil.readEnum(in, FilterList.Operator.class);
			this.subconditions = (in.readBoolean() ? null : (List<Expression>)in.readObject());
		}

		@Override
		public void writeExternal( ObjectOutput out ) throws IOException {
			IOUtil.writeNullable(out, this.oper);
			IOUtil.writeNullable(out, this.subconditions);
		}

		public String toString() {
			StringBuilder str = new StringBuilder();
			str.append("[").append(this.getClass().getSimpleName()).append("=>");
			for (Expression e : this.subconditions) {
				str.append("\n\t").append(e.toString());
			}
			str.append("\n]");
			
			return str.toString();
		}
	}
	
}
