package gov.usgs.cida.geotools.datastore;

import com.vividsolutions.jts.geom.Envelope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.geotools.data.shapefile.ShapefileAttributeReader;
import org.geotools.data.shapefile.dbf.FieldIndexedDbaseFileReader;
import org.geotools.renderer.ScreenMap;
import org.opengis.feature.type.AttributeDescriptor;


/**
 *
 * @author tkunicki
 */
public class DbaseListShapefileAttributeJoiningReader  extends ShapefileAttributeReader {
    
    private final ShapefileAttributeReader delegate;
    private final int shapefileJoinAttributeIndex;
    private final List<FieldIndexedDbaseFileReader> dbaseReaderList;
    
    private final int[] dbaseReaderIndices;
    private final int[] dbaseReaderFieldIndices;
    private final FieldIndexedDbaseFileReader.Row[] dbaseReaderRows;
    
    public DbaseListShapefileAttributeJoiningReader(ShapefileAttributeReader delegate, List<FieldIndexedDbaseFileReader> dbaseReaderList, int shapefileJoinAttributeIndex) throws IOException {
        super(hack(delegate), null, null); // lame duck
        this.delegate = delegate;
        this.shapefileJoinAttributeIndex = shapefileJoinAttributeIndex;
        this.dbaseReaderList = dbaseReaderList;
        
        dbaseReaderRows = new FieldIndexedDbaseFileReader.Row[dbaseReaderList.size()];
        
        int attributeCount = getAttributeCount();
        dbaseReaderIndices = new int[attributeCount];
        dbaseReaderFieldIndices = new int[attributeCount];
        for (int attributeIndex = 0; attributeIndex < attributeCount; ++attributeIndex) {
            AttributeDescriptor attributeDescriptor = getAttributeType(attributeIndex);
            Object dbaseReaderIndexObject = attributeDescriptor.getUserData().get(DbaseDirectoryShapefileDataStore.KEY_READER_INDEX);
            if (dbaseReaderIndexObject instanceof Integer) {
                dbaseReaderIndices[attributeIndex] = (Integer)dbaseReaderIndexObject;
                Object dbaseReaderFieldIndexObject = attributeDescriptor.getUserData().get(DbaseDirectoryShapefileDataStore.KEY_FIELD_INDEX);
                if (dbaseReaderFieldIndexObject instanceof Integer) {
                    dbaseReaderFieldIndices[attributeIndex] = (Integer)dbaseReaderFieldIndexObject;
                } else {
                    dbaseReaderFieldIndices[attributeIndex] = -1;
                }
            } else {
                dbaseReaderIndices[attributeIndex] = -1;
                dbaseReaderFieldIndices[attributeIndex] = -1;
            }
        }
    }

    @Override
    public void close() throws IOException {
        try { delegate.close(); } catch (IOException ignore) { }
        for (FieldIndexedDbaseFileReader dbaseReader : dbaseReaderList) {
            if (dbaseReaderList != null) {
                try { dbaseReader.close(); } catch (IOException ignore) {}
            }
        }
    }

    @Override
    public int getRecordNumber() {
        return delegate.getRecordNumber();
    }

    @Override
    public boolean hasNext() throws IOException {
        return delegate.hasNext();
    }

    @Override
    public void next() throws IOException {
        delegate.next();
        Object indexedValue = delegate.read(shapefileJoinAttributeIndex);
        for (int dbaseReaderIndex = 0, dbaseReaderCount = dbaseReaderList.size();
                dbaseReaderIndex < dbaseReaderCount;
                ++dbaseReaderIndex) {
            FieldIndexedDbaseFileReader dbaseReader = dbaseReaderList.get(dbaseReaderIndex);
            dbaseReaderRows[dbaseReaderIndex] = dbaseReader != null && dbaseReader.setCurrentRecordByValue(indexedValue) ?
                    dbaseReader.readRow() :
                    null;
            
        }
    }

    @Override
    public Object read(int attributeIndex) throws IOException, ArrayIndexOutOfBoundsException {
        int dbaseReaderFieldIndex = dbaseReaderFieldIndices[attributeIndex];
        if (dbaseReaderFieldIndex < 0) {
            return delegate.read(attributeIndex);
        } else {
            FieldIndexedDbaseFileReader.Row dbaseReaderRow = dbaseReaderRows[dbaseReaderIndices[attributeIndex]];
            if (dbaseReaderRow != null) {
                return dbaseReaderRow.read(dbaseReaderFieldIndex);
            }
            return null;
        }
    }

    @Override
    public void setScreenMap(ScreenMap screenMap) {
        delegate.setScreenMap(screenMap);
    }

    @Override
    public void setSimplificationDistance(double distance) {
        delegate.setSimplificationDistance(distance);
    }

    @Override
    public void setTargetBBox(Envelope envelope) {
        delegate.setTargetBBox(envelope);
    }

    private static List<AttributeDescriptor> hack(ShapefileAttributeReader delegate) {
        int attributeCount = delegate.getAttributeCount();
        List<AttributeDescriptor> descriptors = new ArrayList<>(delegate.getAttributeCount());
        for (int attributeIndex = 0; attributeIndex < attributeCount; ++attributeIndex) {
            descriptors.add(delegate.getAttributeType(attributeIndex));
        }
        return descriptors;
    }
    
}
