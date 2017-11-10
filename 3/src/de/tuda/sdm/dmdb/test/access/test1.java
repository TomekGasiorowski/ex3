package de.tuda.sdm.dmdb.test.access;

import de.tuda.sdm.dmdb.access.AbstractTable;
import de.tuda.sdm.dmdb.access.AbstractUniqueIndex;
import de.tuda.sdm.dmdb.access.HeapTable;
import de.tuda.sdm.dmdb.access.exercise.Leaf;
import de.tuda.sdm.dmdb.access.exercise.UniqueBPlusTree;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.SQLVarchar;

public class test1 {
	public static void main(String[] args) {
		AbstractRecord record1 = new Record(2);
		record1.setValue(0, new SQLInteger(1));
		record1.setValue(1, new SQLVarchar("Hello111", 10));
		
		AbstractRecord record2 = new Record(2);
		record2.setValue(0, new SQLInteger(2));
		record2.setValue(1, new SQLVarchar("Hello112", 10));
		
		AbstractRecord record3 = new Record(2);
		record3.setValue(0, new SQLInteger(3));
		record3.setValue(1, new SQLVarchar("Hello113", 10));
		
		AbstractTable table = new HeapTable(record1.clone());
		AbstractUniqueIndex<SQLInteger> index = new UniqueBPlusTree<SQLInteger>(table,0, 1);
		AbstractTable tab = index.getTable();
		
		System.out.println(tab.getRecordCount());
		System.out.println(tab.getPrototype());
		System.out.println(tab.getPrimaryKeys());
		System.out.println(tab.getAttributes());
		index.insert(record2);
		index.print();
	}
}
