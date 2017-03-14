package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbFieldNo;
    private final Type gbFieldType;
    private final int aggregateFieldNo;
    private final Op what;
    private final Map<Field, Integer> countGroupedBy;
    private int noGroupCount;
    private String groupFieldName;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what)
            throws IllegalArgumentException{
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.gbFieldNo = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggregateFieldNo = afield;
        this.what = what;
        this.countGroupedBy = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (gbFieldNo == NO_GROUPING) {
            noGroupCount++;
        } else {
            groupFieldName = tup.getTupleDesc().getFieldName(gbFieldNo);
            Field groupField = tup.getField(gbFieldNo);
            if (!countGroupedBy.containsKey(groupField)) {
                countGroupedBy.put(groupField, 1);
            } else {
                int count = countGroupedBy.get(groupField);
                countGroupedBy.put(groupField, count+1);
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        if (gbFieldNo != NO_GROUPING) {
            TupleDesc tupleDesc = new TupleDesc(
                    new Type[]{gbFieldType, Type.INT_TYPE},
                    new String[]{groupFieldName, what.toString()});
            ArrayList<Tuple> tuples = new ArrayList<Tuple>();
            for (Field group : countGroupedBy.keySet()) {

                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, group);
                tuple.setField(1, new IntField(countGroupedBy.get(group)));
                tuples.add(tuple);
            }
            return new TupleIterator(tupleDesc, tuples);
        } else {
            TupleDesc tupleDesc = new TupleDesc(
                    new Type[]{Type.INT_TYPE},
                    new String[]{what.toString()});
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(noGroupCount));
            ArrayList<Tuple> tuples = new ArrayList<Tuple>();
            tuples.add(tuple);
            return new TupleIterator(tupleDesc, tuples);
        }
    }

}
