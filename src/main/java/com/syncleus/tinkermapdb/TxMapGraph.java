/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.syncleus.tinkermapdb;

import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.impls.tg.MapGraph;
import java.io.IOException;
import org.mapdb.DBMaker;

/**
 *
 * @author me
 */
public class TxMapGraph extends MapGraph implements TransactionalGraph {

    public TxMapGraph() throws IOException {
    }

    public TxMapGraph(DBMaker dbmaker) {
        super(dbmaker);
    }

    public TxMapGraph(String directory) throws IOException {
        super(directory);
    }
    
    @Override public boolean isTransactional() {
        return true;
    }
    
    @Override
    public void stopTransaction(Conclusion cnclsn) {
        
    }

    @Override
    public void commit() {
        db.commit();
    }

    @Override
    public void rollback() {
        db.rollback();
    }
    
}
