/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.syncleus.tinkermapdb;

import com.tinkerpop.blueprints.impls.tg.MapGraph;
import java.io.IOException;
import org.apache.commons.collections.IteratorUtils;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author me
 */
public class ExampleTest {

    @Test public void testMapDBGraph() throws IOException {
        
        MapGraph m = new MapGraph();
        m.addEdge(null, m.addVertex("x"), m.addVertex("y"), "xy");
        assertEquals(2, IteratorUtils.toList(m.getVertices().iterator()).size());
        assertEquals(1, IteratorUtils.toList(m.getEdges().iterator()).size());
        
    }
    
    
}
