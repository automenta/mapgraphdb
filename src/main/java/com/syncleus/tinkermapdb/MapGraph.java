package com.tinkerpop.blueprints.impls.tg;


import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.TransactionalGraph.Conclusion;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.KeyIndexableGraphHelper;
import com.tinkerpop.blueprints.util.PropertyFilteredIterable;
import com.tinkerpop.blueprints.util.StringFactory;
import java.io.File;
import java.io.IOException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;

/**
 * MapDB implementation of the property graph interfaces provided by Blueprints.
 * 
 * TODO make all indexes and property maps of contents top-level maps in the database so changes
 * to their atomic keys and values are minimal
 * 
 * original @author Marko A. Rodriguez (http://markorodriguez.com)
 * 
 */
public class MapGraph implements IndexableGraph, KeyIndexableGraph, Serializable /* TransactionalGraph*/ {

    protected Atomic.Long currentId;
    protected Map<String, Vertex> vertices;
    protected Map<String, Edge> edges;
    protected Map<String, MapIndex> indices;

    protected TinkerKeyIndex<MapVertex> vertexKeyIndex;
    protected TinkerKeyIndex<MapEdge> edgeKeyIndex;
    private boolean persists = true;

    protected void init() {
        currentId = db.createAtomicLong("currentId", 0l);
        vertices = db.createHashMap("vertex").make();
        edges = db.createHashMap("edge").make();
        indices = db.createHashMap("index").make();

        vertexKeyIndex = new TinkerKeyIndex<MapVertex>(MapVertex.class);
        edgeKeyIndex = new TinkerKeyIndex<MapEdge>(MapEdge.class);        
    }
    
    private static final Features FEATURES = new Features();
    public static final Features PERSISTENT_FEATURES, PERSISTENT_FEATURES_TX;

    static {
        FEATURES.supportsDuplicateEdges = true;
        FEATURES.supportsSelfLoops = true;
        FEATURES.supportsSerializableObjectProperty = true;
        FEATURES.supportsBooleanProperty = true;
        FEATURES.supportsDoubleProperty = true;
        FEATURES.supportsFloatProperty = true;
        FEATURES.supportsIntegerProperty = true;
        FEATURES.supportsPrimitiveArrayProperty = true;
        FEATURES.supportsUniformListProperty = true;
        FEATURES.supportsMixedListProperty = true;
        FEATURES.supportsLongProperty = true;
        FEATURES.supportsMapProperty = true;
        FEATURES.supportsStringProperty = true;

        FEATURES.ignoresSuppliedIds = false;
        FEATURES.isWrapper = false;

        FEATURES.supportsIndices = true;
        FEATURES.supportsKeyIndices = true;
        FEATURES.supportsVertexKeyIndex = true;
        FEATURES.supportsEdgeKeyIndex = true;
        FEATURES.supportsVertexIndex = true;
        FEATURES.supportsEdgeIndex = true;
        FEATURES.supportsVertexIteration = true;
        FEATURES.supportsEdgeIteration = true;
        FEATURES.supportsEdgeRetrieval = true;
        FEATURES.supportsVertexProperties = true;
        FEATURES.supportsEdgeProperties = true;

        FEATURES.isPersistent = false;
        FEATURES.supportsTransactions = false;
        FEATURES.supportsThreadedTransactions = false;
        FEATURES.supportsThreadIsolatedTransactions = false;

        PERSISTENT_FEATURES = FEATURES.copyFeatures();
        PERSISTENT_FEATURES.isPersistent = true;
        
        PERSISTENT_FEATURES_TX = PERSISTENT_FEATURES.copyFeatures();
        
        PERSISTENT_FEATURES_TX.supportsTransactions = true;
        PERSISTENT_FEATURES_TX.supportsThreadedTransactions = false;
        PERSISTENT_FEATURES_TX.supportsThreadIsolatedTransactions = false;    
    
    }
    
    public transient DB db;

    /** new MapGraph in DirectByteBuffer outside of HEAP, so Garbage Collector is not affected */
    public static MapGraph inOffHeapMemory() {
        MapGraph m = new MapGraph(DBMaker.newMemoryDirectDB().transactionDisable());
        m.persists = false;
        return m;
    }
    

    
    /** Creates new in-memory database which stores all data on heap without serialization. This mode should be very fast, but data will affect Garbage Collector the same way as traditional Java Collections.  */
    public MapGraph() {
        this(DBMaker.newHeapDB().transactionDisable());
        this.persists = false;
    }
    
    /** creates a new MapGraph given DBMaker builder */
    public MapGraph(DBMaker dbmaker) {
        if (!isTransactional())
            dbmaker.transactionDisable();        
        this.db = dbmaker.make();    
        
        init();
    }       
    
    /** If directory is null, creates new MapGraph in temp file */
    public MapGraph(final String directory) throws IOException {
        DBMaker dbMaker;
        if (directory == null) {            
            dbMaker = DBMaker.newTempFileDB();
        }
        else {
            dbMaker = DBMaker.newFileDB(new File(directory));
        }

        dbMaker.asyncWriteEnable().mmapFileEnableIfSupported().cacheSize(32000000);

        if (!isTransactional())
            dbMaker.transactionDisable();        
        
        persists = true;
        db = dbMaker.make();
        init();
    }

    public boolean isTransactional() {
        return false;
    }
    
    public DB getDB() {
        return db;
    }
    
    public Iterable<Vertex> getVertices(final String key, final Object value) {
        if (vertexKeyIndex.getIndexedKeys().contains(key)) {
            return (Iterable) vertexKeyIndex.get(key, value);
        } else {
            return new PropertyFilteredIterable<Vertex>(key, value, this.getVertices());
        }
    }

    public Iterable<Edge> getEdges(final String key, final Object value) {
        if (edgeKeyIndex.getIndexedKeys().contains(key)) {
            return (Iterable) edgeKeyIndex.get(key, value);
        } else {
            return new PropertyFilteredIterable<Edge>(key, value, this.getEdges());
        }
    }

    public <T extends Element> void createKeyIndex(final String key, final Class<T> elementClass, final Parameter... indexParameters) {
        if (elementClass == null)
            throw ExceptionFactory.classForElementCannotBeNull();

        if (Vertex.class.isAssignableFrom(elementClass)) {
            this.vertexKeyIndex.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            this.edgeKeyIndex.createKeyIndex(key);
        } else {
            throw ExceptionFactory.classIsNotIndexable(elementClass);
        }
        commit();
    }

    public <T extends Element> void dropKeyIndex(final String key, final Class<T> elementClass) {
        if (elementClass == null)
            throw ExceptionFactory.classForElementCannotBeNull();

        if (Vertex.class.isAssignableFrom(elementClass)) {
            this.vertexKeyIndex.dropKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            this.edgeKeyIndex.dropKeyIndex(key);
        } else {
            throw ExceptionFactory.classIsNotIndexable(elementClass);
        }
        commit();
    }

    public <T extends Element> Set<String> getIndexedKeys(final Class<T> elementClass) {
        if (elementClass == null)
            throw ExceptionFactory.classForElementCannotBeNull();

        if (Vertex.class.isAssignableFrom(elementClass)) {
            return this.vertexKeyIndex.getIndexedKeys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return this.edgeKeyIndex.getIndexedKeys();
        } else {
            throw ExceptionFactory.classIsNotIndexable(elementClass);
        }
    }

    public <T extends Element> Index<T> createIndex(final String indexName, final Class<T> indexClass, final Parameter... indexParameters) {
        if (this.indices.containsKey(indexName))
            throw ExceptionFactory.indexAlreadyExists(indexName);

        final MapIndex index = new MapIndex(db, indexName, indexClass);
        this.indices.put(index.getIndexName(), index);
        commit();
        return index;
    }

    public <T extends Element> Index<T> getIndex(final String indexName, final Class<T> indexClass) {
        Index index = this.indices.get(indexName);
        if (null == index)
            return null;
        if (!indexClass.isAssignableFrom(index.getIndexClass()))
            throw ExceptionFactory.indexDoesNotSupportClass(indexName, indexClass);
        else
            return index;
    }

    public Iterable<Index<? extends Element>> getIndices() {
        final List<Index<? extends Element>> list = new ArrayList<Index<? extends Element>>();
        for (Index index : indices.values()) {
            list.add(index);
        }
        return list;
    }

    public void dropIndex(final String indexName) {
        this.indices.remove(indexName);
        commit();
    }


    public <C extends MapVertex> C addVertex(final Object id, Class<? extends C> klass) {
        String idString = null;
        Object vertex;
        if (null != id) {
            idString = id.toString();
            vertex = this.vertices.get(idString);
            if (null != vertex) {
                throw ExceptionFactory.vertexWithIdAlreadyExists(id);
            }
        } else {
            boolean done = false;
            while (!done) {
                idString = this.getNextId();
                vertex = this.vertices.get(idString);
                if (null == vertex)
                    done = true;
            }
        }

        C newVertex;
        try {            
            newVertex = klass.newInstance();
        } catch (Exception ex) {
            throw new RuntimeException("Unable to instantiate " + klass, ex);
        }
        newVertex.init(idString, this);
        this.vertices.put(newVertex.getId().toString(), newVertex);

        commit();
        
        return newVertex;        
    }
    
    public Vertex addVertex(final Object id) {
        return addVertex(id, MapVertex.class);
    }

    public Vertex getVertex(final Object id) {
        if (null == id)
            throw ExceptionFactory.vertexIdCanNotBeNull();

        String idString = id.toString();
        return this.vertices.get(idString);
    }

    public Edge getEdge(final Object id) {
        if (null == id)
            throw ExceptionFactory.edgeIdCanNotBeNull();

        String idString = id.toString();
        return this.edges.get(idString);
    }


    public Iterable<Vertex> getVertices() {
        return new ArrayList<Vertex>(this.vertices.values());
    }

    public Iterable<Edge> getEdges() {
        return new ArrayList<Edge>(this.edges.values());
    }

    public void removeVertex(final Vertex vertex) {
        if (!this.vertices.containsKey(vertex.getId().toString()))
            throw ExceptionFactory.vertexWithIdDoesNotExist(vertex.getId());

        for (Edge edge : vertex.getEdges(Direction.BOTH)) {
            this.removeEdge(edge);
        }

        this.vertexKeyIndex.removeElement((MapVertex) vertex);
        for (Index index : this.getIndices()) {
            if (Vertex.class.isAssignableFrom(index.getIndexClass())) {
                MapIndex<MapVertex> idx = (MapIndex<MapVertex>) index;
                idx.removeElement((MapVertex) vertex);
            }
        }

        this.vertices.remove(vertex.getId().toString());
        
        commit();
    }

    public Edge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {
        if (label == null)
            throw ExceptionFactory.edgeLabelCanNotBeNull();

        String idString = null;
        Edge edge;
        if (null != id) {
            idString = id.toString();
            edge = this.edges.get(idString);
            if (null != edge) {
                throw ExceptionFactory.edgeWithIdAlreadyExist(id);
            }
        } else {
            boolean done = false;
            while (!done) {
                idString = this.getNextId();
                edge = this.edges.get(idString);
                if (null == edge)
                    done = true;
            }
        }

        edge = new MapEdge(db, idString, outVertex, inVertex, label, this);
        this.edges.put(edge.getId().toString(), edge);
        final MapVertex out = (MapVertex) outVertex;
        final MapVertex in = (MapVertex) inVertex;
        out.addOutEdge(label, edge);
        in.addInEdge(label, edge);
        
        commit();
        
        return edge;

    }

    public void removeEdge(final Edge edge) {
        MapVertex outVertex = (MapVertex) edge.getVertex(Direction.OUT);
        MapVertex inVertex = (MapVertex) edge.getVertex(Direction.IN);
        if (null != outVertex && null != outVertex.outEdges) {
            final Set<Edge> edges = outVertex.outEdges.get(edge.getLabel());
            if (null != edges)
                edges.remove(edge);
        }
        if (null != inVertex && null != inVertex.inEdges) {
            final Set<Edge> edges = inVertex.inEdges.get(edge.getLabel());            
            if (null != edges)
                edges.remove(edge);
        }


        this.edgeKeyIndex.removeElement((MapEdge) edge);
        for (Index index : this.getIndices()) {
            if (Edge.class.isAssignableFrom(index.getIndexClass())) {
                MapIndex<MapEdge> idx = (MapIndex<MapEdge>) index;
                idx.removeElement((MapEdge) edge);
            }
        }

        this.edges.remove(edge.getId().toString());
        
        commit();
    }

    public GraphQuery query() {
        return new DefaultGraphQuery(this);
    }


    public String toString() {
        return StringFactory.graphString(this, "vertices:" + this.vertices.size() + " edges:" + this.edges.size());
    }

    public void clear() {
             
        this.vertices.clear();
        this.edges.clear();
        this.indices.clear();
        this.currentId.set(0);
        this.vertexKeyIndex.clear();
        this.edgeKeyIndex.clear();
        commit();
    }

    public void shutdown() {
        if (persists)
            db.close();        
    }

    private String getNextId() {
        String idString;
        while (true) {
            idString = this.currentId.toString();
            long i = this.currentId.incrementAndGet();
            if (null == this.vertices.get(idString) || null == this.edges.get(idString) || i == Long.MAX_VALUE)
                break;
        }
                
        return idString;
    }

    public Features getFeatures() {
        return persists ? PERSISTENT_FEATURES : FEATURES;
    }

    public void stopTransaction(Conclusion cnclsn) {        
    }

    public void commit() {
    }

    public void rollback() {
    }

    protected class TinkerKeyIndex<T extends MapElement> extends MapIndex<T> implements Serializable {

        private final Set<String> indexedKeys;
        

        
        public TinkerKeyIndex(final Class<T> indexClass) {
            super(db, null, indexClass);
             
            indexedKeys = db.createHashSet("indexedKeys_" + indexClass.getSimpleName()).make();
        }
        
        public void clear() {
            indexedKeys.clear();
        }

        public void autoUpdate(final String key, final Object newValue, final Object oldValue, final T element) {
            if (this.indexedKeys.contains(key)) {
                if (oldValue != null)
                    this.remove(key, oldValue, element);
                this.put(key, newValue, element);
            }
        }

        public void autoRemove(final String key, final Object oldValue, final T element) {
            this.remove(key, oldValue, element);            
        }

        public void createKeyIndex(final String key) {
            if (this.indexedKeys.add(key)) {

                if (MapVertex.class.equals(this.indexClass)) {
                    KeyIndexableGraphHelper.reIndexElements(MapGraph.this, MapGraph.this.getVertices(), new HashSet<String>(Arrays.asList(key)));
                } else {
                    KeyIndexableGraphHelper.reIndexElements(MapGraph.this, MapGraph.this.getEdges(), new HashSet<String>(Arrays.asList(key)));
                }
                commit();
            }
        }

        public void dropKeyIndex(final String key) {
            if (!this.indexedKeys.contains(key))
                return;

            this.indexedKeys.remove(key);
            this.index.remove(key);

            commit();
        }

        public Set<String> getIndexedKeys() {
            if (null != this.indexedKeys)
                return new HashSet<String>(this.indexedKeys);
            else
                return Collections.emptySet();
        }
    }

}
