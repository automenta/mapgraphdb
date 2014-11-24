package com.tinkerpop.blueprints.impls.tg;


import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ElementHelper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mapdb.DB;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
abstract class MapElement implements Element, Serializable {

    protected Map<String, Object> properties;
    protected String id;
    transient protected MapGraph graph;
    //protected final DB db;

    public MapElement(DB db) {
        //this.db = db;
    }
    
    protected void init(final String id, final MapGraph graph) {
        this.graph = graph;
        this.id = id;
        this.properties = new HashMap(); //db.createHashMap("element_" + id + "_prop").make(); 
    }

    public Set<String> getPropertyKeys() {
        return new HashSet(this.properties.keySet());
    }

    public <T> T getProperty(final String key) {
        return (T) this.properties.get(key);
    }

    public void setProperty(final String key, final Object value) {
        ElementHelper.validateProperty(this, key, value);
        Object oldValue = this.properties.put(key, value);
        if (this instanceof MapVertex)
            this.graph.vertexKeyIndex.autoUpdate(key, value, oldValue, (MapVertex) this);
        else
            this.graph.edgeKeyIndex.autoUpdate(key, value, oldValue, (MapEdge) this);
    }

    public <T> T removeProperty(final String key) {
        Object oldValue = this.properties.remove(key);
        if (this instanceof MapVertex)
            this.graph.vertexKeyIndex.autoRemove(key, oldValue, (MapVertex) this);
        else
            this.graph.edgeKeyIndex.autoRemove(key, oldValue, (MapEdge) this);
        return (T) oldValue;
    }


    public int hashCode() {
        return this.id.hashCode();
    }

    public String getId() {
        return this.id;
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public void remove() {
        if (this instanceof Vertex) {
            this.graph.removeVertex((Vertex) this);            
        }
        else {
            this.graph.removeEdge((Edge) this);
        }
        
        //helps GC
        this.properties.clear();
        this.properties = null;
    }
}
