package gov.usgs.cida.geotools.datastore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultFeatureReader;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.shapefile.ShapefileAttributeReader;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.FieldIndexedDbaseFileReader;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.SchemaException;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

/**
 *
 * @author tkunicki
 */
public class DbaseShapefileDataStore extends ShapefileDataStore {
    
    public final static String KEY_FIELD_INDEX = "dbaseFieldIndex";
    
    private final URL dbaseFileURL;
    private final String shapefileJoinAttributeName;
    
    private Set<String> shapefileAttributeNames;
    private Set<String> joinedDBaseAttributeNames;
    private Map<Object, Integer> fieldIndexMap;

    public DbaseShapefileDataStore(URI namespaceURI, URL dbaseFileURL, URL shapefileURL, String shapefileJoinAttributeName) throws MalformedURLException, IOException {
        super(shapefileURL, namespaceURI, true, true, ShapefileDataStore.DEFAULT_STRING_CHARSET);
        
        this.dbaseFileURL = dbaseFileURL;
        
        this.shapefileJoinAttributeName = shapefileJoinAttributeName;
        
        // NOTE: if this method is removed from constructor it should be synchronized...
        createDbaseReader();
    }
    
    // NOTE:  not synchronized because this is called in contructor,
    // synchronization of initialization of fileIndexMap is the concern...
    private FieldIndexedDbaseFileReader createDbaseReader() throws IOException {
        File dBaseFile = new File(dbaseFileURL.getFile());
        FileChannel dBaseFileChannel = (new FileInputStream(dBaseFile)).getChannel();
        FieldIndexedDbaseFileReader dbaseReader = new FieldIndexedDbaseFileReader(dBaseFileChannel);
        if (fieldIndexMap == null) {
            dbaseReader.buildFieldIndex(shapefileJoinAttributeName);
            fieldIndexMap = Collections.unmodifiableMap(dbaseReader.getFieldIndex());
        } else {
            dbaseReader.setFieldIndex(fieldIndexMap);
        }
        return dbaseReader;
    }

    @Override
    protected List<AttributeDescriptor> readAttributes() throws IOException {
        List<AttributeDescriptor> shapefileAttributeDescriptors = super.readAttributes();
        
        shapefileAttributeNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
		shapefileAttributeDescriptors.stream().forEach((attributeDescriptor) -> {
			shapefileAttributeNames.add(attributeDescriptor.getLocalName());
		});
        
        List<AttributeDescriptor> dbaseFileAttributeDescriptors;
        
        FieldIndexedDbaseFileReader dbaseReader = null;
        try {
            dbaseReader = createDbaseReader();
        
            DbaseFileHeader dbaseFileHeader = dbaseReader.getHeader();
            int dbaseFieldCount = dbaseFileHeader.getNumFields();
            dbaseFileAttributeDescriptors = new ArrayList<>(dbaseFieldCount - 1);

            AttributeTypeBuilder atBuilder = new AttributeTypeBuilder();

            for (int dbaseFieldIndex = 0; dbaseFieldIndex < dbaseFieldCount; ++dbaseFieldIndex) {
                String dbaseFieldName = dbaseFileHeader.getFieldName(dbaseFieldIndex);
                if (!shapefileAttributeNames.contains(dbaseFieldName)) {
                    dbaseFileAttributeDescriptors.add(atBuilder.
                        userData(KEY_FIELD_INDEX, dbaseFieldIndex).
                        binding(dbaseFileHeader.getFieldClass(dbaseFieldIndex)).
                        buildDescriptor(dbaseFileHeader.getFieldName(dbaseFieldIndex)));
                }
            }
        } finally {
            if (dbaseReader != null) {
                try { dbaseReader.close(); } catch (IOException ignore) {}
            }
        }
            
        joinedDBaseAttributeNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
		dbaseFileAttributeDescriptors.stream().forEach((attributeDescriptor) -> {
			joinedDBaseAttributeNames.add(attributeDescriptor.getLocalName());
		});
            
        List<AttributeDescriptor> attributeDescriptors = new ArrayList<>(
                shapefileAttributeDescriptors.size() +
                dbaseFileAttributeDescriptors.size());
        attributeDescriptors.addAll(shapefileAttributeDescriptors);
        attributeDescriptors.addAll(dbaseFileAttributeDescriptors);

        return attributeDescriptors;
    }
    
    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(String typeName, Query query) throws IOException {
        if (requiresShapefileAttributes(query)) {
            if (requiresJoinedDbaseAttributes(query)) {
                // make sure join attribute is in property list if we need to join!
                String[] properties = query.getPropertyNames();
                int joinIndex = indexOfIgnoreCase(properties, shapefileJoinAttributeName);
                if (joinIndex == -1) {
                    int tailIndex = properties.length;
                    properties = Arrays.copyOf(properties, tailIndex + 1);
                    properties[tailIndex] = shapefileJoinAttributeName;
                    query.setPropertyNames(properties);
                }
            }
            return super.getFeatureReader(typeName, query);
        } else {
            try {
                List<String> propertyNames = Arrays.asList(query.getPropertyNames());
                SimpleFeatureType subTypeSchema = DataUtilities.createSubType(getSchema(), propertyNames.toArray(new String[0]));
                return new DefaultFeatureReader(new DbaseAttributeReader(createDbaseReader(), subTypeSchema), subTypeSchema);
            } catch (SchemaException ex) {
                // hack
                throw new IOException(ex);
            }
        }
    }

    @Override
    protected ShapefileAttributeReader getAttributesReader(boolean readDBF, Query query, String[] properties) throws IOException {
        if (requiresJoinedDbaseAttributes(query)) {
            int shapefileJoinAttributeIndex = indexOfIgnoreCase(properties, shapefileJoinAttributeName);
            return new DbaseShapefileAttributeJoiningReader(super.getAttributesReader(true, query, properties), createDbaseReader(), shapefileJoinAttributeIndex);
        } else {
            return super.getAttributesReader(readDBF, query, properties);
        }
    }
    
    private boolean requiresShapefileAttributes(Query query) {
        return QueryUtil.requiresAttributes(query, shapefileAttributeNames);
    }
    
    private boolean requiresJoinedDbaseAttributes(Query query) {
        return QueryUtil.requiresAttributes(query, joinedDBaseAttributeNames);
    }
    
    @Override
    protected String createFeatureTypeName() {
        String path = dbaseFileURL.getPath();
        File file = new File(path);
        String name = file.getName();
        int suffixIndex = name.lastIndexOf("dbf");
        if (suffixIndex > -1) {
            name = name.substring(0, suffixIndex -1);
        }
        name = name.replace(',', '_'); // Are there other characters?
        return name;
    }

    @Override
    public ReferencedEnvelope getBounds(Query query) throws IOException {
        return super.getBounds(query);
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    private int indexOfIgnoreCase(String[] strings, String string) {
        return indexOfIgnoreCase(Arrays.asList(strings), string);
    }
    
    private int indexOfIgnoreCase(Collection<String> strings, String string) {
        int index = 0;
        for (String candidate : strings) {
            if (string.equalsIgnoreCase(candidate)) {
                return index;
            }
            index++;
        }
        return -1;
    }
}

