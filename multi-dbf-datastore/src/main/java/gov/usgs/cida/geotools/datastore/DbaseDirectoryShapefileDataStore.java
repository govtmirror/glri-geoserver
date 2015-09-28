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
import java.util.Collections;
import java.util.HashMap;
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
public class DbaseDirectoryShapefileDataStore extends ShapefileDataStore {
    
    public final static String KEY_FIELD_INDEX = "dbaseFieldIndex";
    public final static String KEY_READER_INDEX = "dbaseReaderIndex";
    
    private final URL joinedDBaseDirectoryURL;
    private final String shapefileJoinAttributeName;
    private final List<File> joinableDbaseFiles;

    private Set<String> shapefileAttributeNames;
    private Set<String> joinedDBaseAttributeNames;
	private static final Charset UTF8 = Charset.forName("UTF-8");
    
    private final Map<File, Map<Object, Integer>> fileFieldIndexMap = new HashMap<>();
    
    public DbaseDirectoryShapefileDataStore(URI namespaceURI, URL dbaseDirectoryURL, URL shapefileURL, String shapefileJoinAttributeName) throws MalformedURLException, IOException {
        super(shapefileURL, namespaceURI, true, true, UTF8);
        
        this.joinedDBaseDirectoryURL = dbaseDirectoryURL;
        
        this.shapefileJoinAttributeName = shapefileJoinAttributeName;
        
        joinableDbaseFiles = createJoinableDbaseFileList();
        if (joinableDbaseFiles.isEmpty()) {
            throw new IllegalArgumentException("no joinable dbf files on field " + shapefileJoinAttributeName + " in " + dbaseDirectoryURL);
        }
        
        // NOTE: if this method is removed from constructor it should be synchronized...
        createDbaseReaderList();
    }
    
    // NOTE:  not synchronized because this is called in contructor,
    // synchronization of initialization of fileIndexMap is the concern...
    private List<FieldIndexedDbaseFileReader> createDbaseReaderList() throws IOException {
        List<FieldIndexedDbaseFileReader> dbaseReaderList = new ArrayList<>(joinableDbaseFiles.size());
        for (File dbaseFile : joinableDbaseFiles) {
			LOGGER.log(Level.FINER, "DbaseDirectoryShapefileDataStore reading {0}", dbaseFile.getPath());
            FileChannel dbaseFileChannel = (new FileInputStream(dbaseFile)).getChannel();
            FieldIndexedDbaseFileReader dbaseReader = new FieldIndexedDbaseFileReader(dbaseFileChannel);
            Map<Object, Integer> fieldIndexMap = fileFieldIndexMap.get(dbaseFile);
            if (fieldIndexMap == null) {
                dbaseReader.buildFieldIndex(shapefileJoinAttributeName);
                fileFieldIndexMap.put(dbaseFile, Collections.unmodifiableMap(dbaseReader.getFieldIndex()));
            } else {
                dbaseReader.setFieldIndex(fieldIndexMap);
            }
            dbaseReaderList.add(dbaseReader);
        }
        return dbaseReaderList;
    }
    
    @Override
    protected List<AttributeDescriptor> readAttributes() throws IOException {
        List<AttributeDescriptor> shapefileAttributeDescriptors = super.readAttributes();
        
        shapefileAttributeNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
		shapefileAttributeDescriptors.stream().forEach((attributeDescriptor) -> {
			shapefileAttributeNames.add(attributeDescriptor.getLocalName());
		});
        
        ArrayList<AttributeDescriptor> dbaseFileAttributeDescriptors = new ArrayList<>();
        joinedDBaseAttributeNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        
        List<FieldIndexedDbaseFileReader> dbaseReaderList = createDbaseReaderList();
        int dbaseReaderCount = dbaseReaderList.size();
        for (int dbaseReaderIndex = 0; dbaseReaderIndex < dbaseReaderCount; ++dbaseReaderIndex) {
            
            FieldIndexedDbaseFileReader dbaseReader = dbaseReaderList.get(dbaseReaderIndex);
            try {

                DbaseFileHeader dbaseFileHeader = dbaseReader.getHeader();
                int dbaseFieldCount = dbaseFileHeader.getNumFields();

                AttributeTypeBuilder atBuilder = new AttributeTypeBuilder();

                for (int dbaseFieldIndex = 0; dbaseFieldIndex < dbaseFieldCount; ++dbaseFieldIndex) {
                    String dbaseFieldName = dbaseFileHeader.getFieldName(dbaseFieldIndex);
                    if (!shapefileAttributeNames.contains(dbaseFieldName) && !joinedDBaseAttributeNames.contains(dbaseFieldName)) {
                        dbaseFileAttributeDescriptors.add(atBuilder.
                            userData(KEY_READER_INDEX, dbaseReaderIndex).
                            userData(KEY_FIELD_INDEX, dbaseFieldIndex).
                            binding(dbaseFileHeader.getFieldClass(dbaseFieldIndex)).
                            buildDescriptor(dbaseFileHeader.getFieldName(dbaseFieldIndex)));
                    }
                }

				dbaseFileAttributeDescriptors.stream().forEach((attributeDescriptor) -> {
					joinedDBaseAttributeNames.add(attributeDescriptor.getLocalName());
				});

            } finally {
                if (dbaseReader != null) {
                    try { dbaseReader.close(); } catch (IOException ignore) {}
                }
            }
        }
        
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
                int joinIndex = Arrays.asList(properties).indexOf(shapefileJoinAttributeName);
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
                SimpleFeatureType subTypeSchema = DataUtilities.createSubType(getSchema(), propertyNames.toArray(new String[propertyNames.size()]));
                return new DefaultFeatureReader(new DbaseListAttributeReader(createDbaseReaderList(), subTypeSchema), subTypeSchema);
            } catch (SchemaException ex) {
                // hack
                throw new IOException(ex);
            }
        }
    }

    @Override
    protected ShapefileAttributeReader getAttributesReader(boolean readDBF, Query query, String[] properties) throws IOException {
        if (requiresJoinedDbaseAttributes(query)) {
            int shapefileJoinAttributeIndex = Arrays.asList(properties).indexOf(shapefileJoinAttributeName);
            return new DbaseListShapefileAttributeJoiningReader(super.getAttributesReader(true, query, properties), createDbaseReaderList(), shapefileJoinAttributeIndex);
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
        String path = joinedDBaseDirectoryURL.getPath();
        File file = new File(path);
        String name = file.getName();
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
    
    private List<File> createJoinableDbaseFileList() throws IOException {
        if(!"file".equals(joinedDBaseDirectoryURL.getProtocol())) {
            throw new IllegalArgumentException("only url \"file\" protocols accepted");
        }
        File urlAsFile = new File(joinedDBaseDirectoryURL.getPath());
        if (!urlAsFile.canRead()) {
            throw new IllegalArgumentException("can't read " + joinedDBaseDirectoryURL);
        }
        if (!urlAsFile.isDirectory()) {
            throw new IllegalArgumentException(joinedDBaseDirectoryURL + " must be a directory");
        }
        List<File> _joinableDbaseFiles = new ArrayList<>();
        for (File child : urlAsFile.listFiles()) {
            if (child.getName().toLowerCase().endsWith("dbf")) {
                FieldIndexedDbaseFileReader dbaseReader = null;
                try {
                    dbaseReader = new FieldIndexedDbaseFileReader((new FileInputStream(child)).getChannel());
                    DbaseFileHeader header = dbaseReader.getHeader();
                    if (header.getNumRecords() > 0) {
                        int fieldCount = header.getNumFields();
                        for (int fieldIndex = 0; fieldIndex < fieldCount && !_joinableDbaseFiles.contains(child); ++fieldIndex) {
                            if (shapefileJoinAttributeName.equalsIgnoreCase(header.getFieldName(fieldIndex))) {
                                _joinableDbaseFiles.add(child);
                            }
                        }
                    }
                } catch (IOException e) {
                    
                } finally {
                    if (dbaseReader != null) {
                        try { dbaseReader.close(); } catch (IOException ignore) {}
                    }
                }
            }
        }
        return Collections.unmodifiableList(_joinableDbaseFiles);
    }

}

