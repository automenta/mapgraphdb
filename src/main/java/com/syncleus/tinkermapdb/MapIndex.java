package com.tinkerpop.blueprints.impls.tg;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.WrappingCloseableIterable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.mapdb.DB;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * 
 * TODO add garbage collect to remove index keys with empty maps
 */
class MapIndex<T extends Element> implements Index<T>, Serializable {

    protected Map<String, Map<Object, Set<T>>> index;
    protected final String indexName;
    protected final Class<T> indexClass;
    private final String dbIndexName;
    private final DB db;

    public MapIndex(final DB db, final String indexName, final Class<T> indexClass) {
        this.db = db;
        this.dbIndexName = "index_" + (indexName!=null ? indexName : "") + "_" + indexClass.getSimpleName();
        this.index = db.createHashMap(dbIndexName).make();
        this.indexName = indexName;
        this.indexClass = indexClass;
    }

    public String getIndexName() {
        return this.indexName;
    }

    public Class<T> getIndexClass() {
        return this.indexClass;
    }

    public void put(final String key, final Object value, final T element) {
        Map<Object, Set<T>> keyMap = this.index.get(key);
        if (keyMap == null) {
            keyMap = db.createHashMap(dbIndexName + "_" + key).make(); //new HashMap<Object, Set<T>>();
            this.index.put(key, keyMap);
        }
        Set<T> objects = keyMap.get(value);
        if (null == objects) {
            //objects = db.createHashSet(dbIndexName + "_" + key + "_" + value).make(); //new HashSet<T>();
            objects = new LinkedHashSet();
            
            keyMap.put(value, objects);
        }
        objects.add(element);

    }
    
    public final WrappingCloseableIterable<T> emptyClosableIterator = new WrappingCloseableIterable<T>((Iterable) Collections.emptyList());

    public CloseableIterable<T> get(final String key, final Object value) {
        final Map<Object, Set<T>> keyMap = this.index.get(key);
        if (null == keyMap) {
            return emptyClosableIterator;
        } else {
            Set<T> set = keyMap.get(value);
            if (null == set)
                return emptyClosableIterator;
            else {                
                return new WrappingCloseableIterable<T>(new ArrayList<T>(set));
            }
        }
    }

    public CloseableIterable<T> query(final String key, final Object query) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    public long count(final String key, final Object value) {
        final Map<Object, Set<T>> keyMap = this.index.get(key);
        if (null == keyMap) {
            return 0;
        } else {
            Set<T> set = keyMap.get(value);
            if (null == set)
                return 0;
            else
                return set.size();
        }
    }

    public void remove(final String key, final Object value, final T element) {
        final Map<Object, Set<T>> keyMap = this.index.get(key);
        if (null != keyMap) {
            Set<T> objects = keyMap.get(value);
            if (null != objects)
                if (objects.remove(element))
                    if (objects.isEmpty())
                        keyMap.remove(value);                                
        }
    }

    public void removeElement(final T element) {
        if (this.indexClass.isAssignableFrom(element.getClass())) {
            for (Map<Object, Set<T>> map : index.values()) {                
                for (Set<T> set : map.values()) {
                    set.remove(element);
                }
            }
        }
    }

    public String toString() {
        return StringFactory.indexString(this);
    }
}
