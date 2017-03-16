package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tupleDesc;
    private RecordId recordId;
    private Field[] fields;

    public static Tuple merge(Tuple t1, Tuple t2) {
        Tuple tuple = new Tuple(TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc()));
        tuple.recordId = null;
        System.arraycopy(t1.fields, 0, tuple.fields, 0, t1.getTupleDesc().numFields());
        System.arraycopy(t2.fields, 0, tuple.fields, t1.getTupleDesc().numFields(),
                t2.getTupleDesc().numFields());
        return tuple;
    }

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        tupleDesc = td;
        fields = new Field[td.numFields()];
    }

    public void resetTupleDesc(TupleDesc td) {
        this.tupleDesc = td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        String result = "";
        for (int i = 0; i < tupleDesc.numFields(); ++i) {
            if (i > 0) {
                result += "\t";
            }
            result += getField(i).toString();
        }
        result += "\n";
        return result;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return Arrays.asList(fields).iterator();
    }

    public boolean equals(Object o) {
        if (o == null || o.getClass() != this.getClass()) {
            return false;
        }
        Tuple that = (Tuple) o;
        return Arrays.equals(this.fields, that.fields) && this.recordId.equals(that.recordId)
                && this.tupleDesc.equals(that.tupleDesc);
    }
}
