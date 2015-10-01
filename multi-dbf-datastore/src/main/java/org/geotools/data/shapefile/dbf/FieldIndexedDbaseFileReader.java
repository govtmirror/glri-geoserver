package org.geotools.data.shapefile.dbf;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.resources.NIOUtilities;

/**
 * Adds a method to DbaseFileReader to index one unique column for quick mapping
 * from the values in that column to the row they occur on.
 * 
 * NOTE:  must remain in org.geotools.data.shapefile.dbf due to access of
 * access of package protected variables in super class 
 * 
 * @author eeverman
 */
public class FieldIndexedDbaseFileReader extends DbaseFileReader {
        
    private static final Logger LOGGER = org.geotools.util.logging.Logging.getLogger(FieldIndexedDbaseFileReader.class);
	
	private static final Boolean USE_MEMMAPPED_BUFFER = true;
	
	/**
	 * Index of the join column values to the unique row they occur on.
	 * The rows are ONE based.
	 */
	Map<Object, Integer> indexMap = new HashMap<>();
    
    public FieldIndexedDbaseFileReader(FileChannel fileChannel) throws IOException {
        super(fileChannel, USE_MEMMAPPED_BUFFER, ShapefileDataStore.DEFAULT_STRING_CHARSET);
    }
    
    public FieldIndexedDbaseFileReader(FileChannel fileChannel, boolean useMemoryMappedBuffer) throws IOException {
        super(fileChannel, useMemoryMappedBuffer, ShapefileDataStore.DEFAULT_STRING_CHARSET, TimeZone.getDefault());
    }
    
    public FieldIndexedDbaseFileReader(FileChannel fileChannel, boolean useMemoryMappedBuffer, Charset stringCharset)
            throws IOException {
        super(fileChannel, useMemoryMappedBuffer, stringCharset, TimeZone.getDefault());
    }

    public FieldIndexedDbaseFileReader(FileChannel fileChannel, boolean useMemoryMappedBuffer, Charset stringCharset, TimeZone timeZone)
            throws IOException {
        super(fileChannel, useMemoryMappedBuffer, stringCharset, timeZone);
    }
	
	/**
	 * Jump to the correct record based on a record number, which is ONE based.
	 * 
	 * Everman:  In Tom K.'s original code, I think the intention was that this
	 * be 0 based, however, it was inconsistent in that usage which resulted
	 * in bad and skipped values for the first and last records.  This current
	 * fix will continue the basic functionality which IS and WAS ONE based,
	 * however, the bounds checking has been fixed.
	 * 
	 * According to Tom's notes, he copied this from IndexedDBaseFileReader.goTo(...)
	 * however, it doesn't look much like that code.
	 * 
	 * @param recordNumber One based record number.
	 * @throws IOException
	 * @throws UnsupportedOperationException
	 */
    public void setCurrentRecordByNumber(int recordNumber) throws IOException, UnsupportedOperationException {
        if (recordNumber > header.getNumRecords()) {
            throw new IllegalArgumentException("The recordNumber was greater than the recordCount");
        } else if (recordNumber < 1) {
			throw new IllegalArgumentException("The recordNumber is ONE based, but a call was made with a smaller value");
		}
        
        long newPosition = this.header.getHeaderLength()
                + this.header.getRecordLength() * (long) (recordNumber - 1);

        if (this.useMemoryMappedBuffer) {
            if(newPosition < this.currentOffset || (this.currentOffset + buffer.limit()) < (newPosition + header.getRecordLength())) {
                NIOUtilities.clean(buffer);
                FileChannel fc = (FileChannel) channel;
                if(fc.size() > newPosition + Integer.MAX_VALUE) {
                    currentOffset = newPosition;
                } else {
                    currentOffset = fc.size() - Integer.MAX_VALUE;
                }
                buffer = fc.map(FileChannel.MapMode.READ_ONLY, currentOffset, Integer.MAX_VALUE);
                buffer.position((int) (newPosition - currentOffset));
            } else {
                buffer.position((int) (newPosition - currentOffset));
            }
        } else {
            if (this.currentOffset <= newPosition
                    && this.currentOffset + buffer.limit() >= newPosition) {
                buffer.position((int) (newPosition - this.currentOffset));
            } else {
                FileChannel fc = (FileChannel) this.channel;
                fc.position(newPosition);
                this.currentOffset = newPosition;
                buffer.limit(buffer.capacity());
                buffer.position(0);
                fill(buffer, fc);
                buffer.position(0);
            }
        }
    }
    
    public boolean setCurrentRecordByValue(Object value) throws IOException {
        if (indexMap == null || indexMap.isEmpty()) {
            throw new IllegalArgumentException("index not created");
        }
        if (!indexMap.containsKey(value)) {
            return false;
        }
        setCurrentRecordByNumber(indexMap.get(value));
        return true;
    }
    
    
    public void buildFieldIndex(int fieldIndex) throws IOException {
        int fieldCount = header.getNumFields();
        if (!(fieldIndex < fieldCount)) {
            throw new IllegalArgumentException("fieldIndex " + fieldIndex +  " >= " + fieldCount);
        }
        setCurrentRecordByNumber(1);
        indexMap.clear();
		
		int recordNumber = 1;	//We want to keep the number for checking when done;
		
        for (recordNumber = 1; hasNext(); recordNumber++) {
            read(); // required when using readField
            Object value = readField(fieldIndex);
			indexMap.put(value, recordNumber);
        }
		
		//Rather than checking for duplicates as we add, just log one message
		//at the end.  recordNumber is always advanced one past the number of records.
		if (indexMap.size() < (recordNumber -1)) {
			LOGGER.log(
					Level.WARNING,
					"A dbf file contains non-unique values in the {0} column, "
							+ "which is used to join to a shapefile. "
							+ "Only the last value will be used.  Number of non-unique records: {1}",
					new Object[] {
						header.getFieldName(fieldIndex),
						(recordNumber - 1) - indexMap.size()
					});
		}
    }
    
    public void buildFieldIndex(String fieldName) throws IOException {
        for (int fieldIndex = 0, fieldCount = header.getNumFields(); fieldIndex < fieldCount; ++fieldIndex) {
            if (header.getFieldName(fieldIndex).equalsIgnoreCase(fieldName)) {
                buildFieldIndex(fieldIndex);
                return;
            }
        }
        throw new IllegalArgumentException("field " + fieldName + " not found in dbf");
    }
    
    public Map<Object, Integer> getFieldIndex() {
        return indexMap;
    }
    
    public void setFieldIndex(Map<Object, Integer> index) {
        if (index == null || index.isEmpty()) {
            throw new IllegalArgumentException("index is null or empty");
        }
//        if (index.size() != header.getNumRecords()) {
//            throw new IllegalArgumentException("index size greater than record count");
//        }
        // TODO:  don't want to be this lenient...  see notes in buildFileIndes(int)
        if (index.size() > header.getNumRecords()) {
            throw new IllegalArgumentException("index size greater than record count");
        } else if (index.size() < header.getNumRecords()) {
            LOGGER.log(
                Level.WARNING,
                "index count <  record count.  Most likely due to index creation on field w/ non-unique values.");
        }
        this.indexMap = index;
    }
}
