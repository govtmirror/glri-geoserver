package gov.usgs.cida.geotools.datastore;

import com.vividsolutions.jts.geom.Envelope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.geotools.data.shapefile.ShapefileAttributeReader;
import org.geotools.data.shapefile.dbf.FieldIndexedDbaseFileReader;
import org.geotools.renderer.ScreenMap;
import org.opengis.feature.type.AttributeDescriptor;


/**
 *
 * @author tkunicki
 */
public class DbaseShapefileAttributeJoiningReader  extends ShapefileAttributeReader {
    private static final Logger LOGGER = org.geotools.util.logging.Logging.getLogger(DbaseShapefileAttributeJoiningReader.class);
    private final ShapefileAttributeReader delegate;
    private final int shapefileJoinAttributeIndex;
    private final FieldIndexedDbaseFileReader dbaseReader;
    
    private FieldIndexedDbaseFileReader.Row dbaseRow;
    private int[] dbaseFieldIndices;
    
    public DbaseShapefileAttributeJoiningReader(ShapefileAttributeReader delegate, FieldIndexedDbaseFileReader dbaseReader, int shapefileJoinAttributeIndex) throws IOException {
        super(hack(delegate), null, null); // lame duck
        this.delegate = delegate;
        this.shapefileJoinAttributeIndex = shapefileJoinAttributeIndex;
        this.dbaseReader = dbaseReader;
        
        int attributeCount = getAttributeCount();
        dbaseFieldIndices = new int[attributeCount];
        for (int attributeIndex = 0; attributeIndex < attributeCount; ++attributeIndex) {
            Object o = getAttributeType(attributeIndex).getUserData().get(DbaseShapefileDataStore.KEY_FIELD_INDEX);
            dbaseFieldIndices[attributeIndex] = o instanceof Integer ? (Integer) o : -1;
        }
    }

    @Override
    public void close() throws IOException {
        try { delegate.close(); } catch (IOException ignore) { }
        try { dbaseReader.close(); } catch (IOException ignore) { }
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
		LOGGER.finest("Calling delegate.next()");
        delegate.next();
		LOGGER.finest("Calling delegate.read()");
		Object record = delegate.read(shapefileJoinAttributeIndex);
		LOGGER.finest("Calling dbaseReader.setCurrentRecordByValue");
        if (dbaseReader.setCurrentRecordByValue(record)) {
			LOGGER.finest("Calling dbaseReader.readRow");
            dbaseRow = dbaseReader.readRow();
        } else {
			LOGGER.finest("dbaseRow = null");
            dbaseRow = null;
        }
		LOGGER.finest("dbaseReader.next() completed");
    }

    @Override
    public Object read(int attributeIndex) throws IOException, ArrayIndexOutOfBoundsException {
        int dBaseFieldIndex = dbaseFieldIndices[attributeIndex];
        if (dBaseFieldIndex < 0) {
            return delegate.read(attributeIndex);
        } else {
            return dbaseRow == null ? null : dbaseRow.read(dBaseFieldIndex);
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
