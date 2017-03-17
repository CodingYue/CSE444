package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbFieldNo;
    private final Type gbFieldType;
    private final int aggregateFieldNo;
    private final Op what;

    private final Map<Field, Integer> countGroupedBy;
    private final Map<Field, Integer> valueGroupedBy;

    private String groupFieldName;
    /**
     * Aggregate constructor
     * 
     * @param gbFieldNo
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param aggregateFieldNo
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbFieldNo, Type gbfieldtype, int aggregateFieldNo, Op what) {
        this.gbFieldNo = gbFieldNo;
        this.gbFieldType = gbfieldtype;
        this.aggregateFieldNo = aggregateFieldNo;
        this.what = what;
        this.countGroupedBy = new HashMap<Field, Integer>();
        this.valueGroupedBy = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = gbFieldNo == NO_GROUPING ? new IntField(0): tup.getField(gbFieldNo);
        groupFieldName = gbFieldNo == NO_GROUPING ? null : tup.getTupleDesc().getFieldName(gbFieldNo);
        IntField aggregateField = (IntField) tup.getField(aggregateFieldNo);
        int countAddition = what == Op.SC_AVG ? ((IntField) tup.getField(aggregateFieldNo+1)).getValue() : 1;
        if (!countGroupedBy.containsKey(groupField)) {
            countGroupedBy.put(groupField, countAddition);
            valueGroupedBy.put(groupField, aggregateField.getValue());
        } else {
            countGroupedBy.put(groupField, countGroupedBy.get(groupField)+countAddition);
            Integer value = valueGroupedBy.get(groupField);
            Integer aggregateValue = aggregateField.getValue();
            switch (what) {
                case AVG:
                case SUM_COUNT:
                case SUM:
                    valueGroupedBy.put(groupField, value + aggregateValue);
                    break;
                case MAX:
                    valueGroupedBy.put(groupField, Math.max(value, aggregateValue));
                    break;
                case MIN:
                    valueGroupedBy.put(groupField, Math.min(value, aggregateValue));
                    break;
                case COUNT:
                default:
                    break;
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        boolean hasGroup = gbFieldNo != NO_GROUPING;

        TupleDesc tupleDesc;
        if (hasGroup) {
            if (what == Op.SUM_COUNT) {
                tupleDesc = new TupleDesc(
                        new Type[]{gbFieldType, Type.INT_TYPE, Type.INT_TYPE},
                        new String[]{groupFieldName, "sum", "count"});
            } else {
                tupleDesc = new TupleDesc(
                        new Type[]{gbFieldType, Type.INT_TYPE},
                        new String[]{groupFieldName, what.toString()});
            }
        } else {
            if (what == Op.SUM_COUNT) {
                tupleDesc = new TupleDesc(
                        new Type[]{Type.INT_TYPE, Type.INT_TYPE},
                        new String[]{"sum", "count"});
            } else {
                tupleDesc = new TupleDesc(
                        new Type[]{Type.INT_TYPE},
                        new String[]{what.toString()});
            }
        }
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        for (Field group : countGroupedBy.keySet()) {

            Tuple tuple = new Tuple(tupleDesc);
            if (hasGroup) {
                tuple.setField(0, group);
            }
            tuple.setField(hasGroup ? 1:0, new IntField(getAggregateValue(group)[0]));
            if (what == Op.SUM_COUNT) {
                tuple.setField(hasGroup ? 2:1, new IntField(getAggregateValue(group)[1]));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(tupleDesc, tuples);
    }

    private Integer[] getAggregateValue(Field group) {
        switch (what) {
            case AVG:
            case SC_AVG:
                return new Integer[]{valueGroupedBy.get(group) / countGroupedBy.get(group)};
            case COUNT:
                return new Integer[]{countGroupedBy.get(group)};
            case MAX:
            case MIN:
            case SUM:
                return new Integer[]{valueGroupedBy.get(group)};
            case SUM_COUNT:
                return new Integer[]{valueGroupedBy.get(group), countGroupedBy.get(group)};
            default:
                break;
        }
        return new Integer[]{};
    }
}