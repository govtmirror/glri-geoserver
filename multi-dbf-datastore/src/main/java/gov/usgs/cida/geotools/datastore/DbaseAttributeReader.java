package gov.usgs.cida.geotools.datastore;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.geotools.data.AttributeReader;
import org.geotools.data.shapefile.dbf.FieldIndexedDbaseFileReader;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

/**
 *
 * @author tkunicki
 */
public class DbaseAttributeReader implements AttributeReader {
    
    private final SimpleFeatureType featureType;
    private final FieldIndexedDbaseFileReader dbaseReader;
    private final int attributeCount;
    
    private final int[] dbaseReaderFieldIndices;
    private FieldIndexedDbaseFileReader.Row dbaseReaderRow;

    DbaseAttributeReader(FieldIndexedDbaseFileReader dbaseReader, SimpleFeatureType featureType) throws IOException {
        this.featureType = featureType;
        this.dbaseReader = dbaseReader;
        this.attributeCount = featureType.getAttributeCount();
        
        dbaseReaderFieldIndices = new int[attributeCount];
        for (int attributeIndex = 0; attributeIndex < attributeCount; ++attributeIndex) {
            Object o = featureType.getDescriptor(attributeIndex).getUserData().get(DbaseShapefileDataStore.KEY_FIELD_INDEX);
            dbaseReaderFieldIndices[attributeIndex] = o instanceof Integer ? (Integer) o : -1;
        }
    }

    @Override
    public int getAttributeCount() {
        return featureType.getAttributeCount();
    }

    @Override
    public AttributeDescriptor getAttributeType(int index) throws ArrayIndexOutOfBoundsException {
        return featureType.getDescriptor(index);
    }

    @Override
    public void close() throws IOException {
        dbaseReader.close();
    }

    @Override
    public boolean hasNext() throws IOException {
        return dbaseReader.hasNext();
    }

    @Override
    public void next() throws IOException, IllegalArgumentException, NoSuchElementException {
        dbaseReaderRow = dbaseReader.readRow();
    }

    @Override
    public Object read(int index) throws IOException, ArrayIndexOutOfBoundsException {
        if (index < attributeCount) {
            return dbaseReaderRow.read(dbaseReaderFieldIndices[index]);
        } else {
            throw new ArrayIndexOutOfBoundsException(index);
        }
    }
}
