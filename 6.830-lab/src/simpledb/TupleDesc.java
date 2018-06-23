package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
    private Type[] typeAr;
    private String[] fieldAr;

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int l1 = td1.typeAr.length;
        int l2 = td2.typeAr.length;

        Type[] ta = new Type[l1+l2];
        System.arraycopy(td1.typeAr, 0, ta, 0, l1);
        System.arraycopy(td2.typeAr, 0, ta, l1, l2);
        TupleDesc td = new TupleDesc(ta);

        td.fieldAr = new String[l1+l2];
        System.arraycopy(td1.fieldAr, 0, td.fieldAr, 0, l1);
        System.arraycopy(td2.fieldAr, 0, td.fieldAr, l1, l2);

        return td;
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this.typeAr = typeAr;
        this.fieldAr = fieldAr;
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.typeAr = typeAr;
        this.fieldAr = new String[typeAr.length];
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return typeAr.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= numFields() || i < 0)
            throw new NoSuchElementException();
        return fieldAr[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        // some code goes here
        System.out.println(name);
        if (name == null)
            throw new NoSuchElementException();
        
        boolean allNull = true;
        for(int i = 0;i < numFields();i++)
            if (fieldAr[i] != null) {
                allNull = false;
                break;
            }
        if (allNull)
            throw new NoSuchElementException();

        for(int i = 0;i < numFields();i++)
            if (fieldAr[i].equals(name)) 
                return i;
        throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= numFields() || i < 0)
            throw new NoSuchElementException();
        return typeAr[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int sz = 0;
        for (Type t: typeAr)
            sz += t.getLen();
        return sz;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (o == null)
            return false;
        if (!(o instanceof TupleDesc))
            return false;

        TupleDesc td = (TupleDesc)o;
        if (getSize() != td.getSize())
            return false;
        int i = 0;
        for (;i < numFields() && i < td.numFields();i++) 
            if (typeAr[i] != td.typeAr[i])
                return false;
        if (i != numFields())
            return false;
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuffer sb = new StringBuffer("");
        for (int i = 0;i < numFields();i++) {
            sb.append(typeAr[i] == Type.INT_TYPE ? "int" : "string");
            sb.append("(" + fieldAr[i] + ")");
            if (i < numFields()-1)
                sb.append(", ");
        }
        return sb.toString();
    }
}
