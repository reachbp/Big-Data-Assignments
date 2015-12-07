package rdfjena;

import org.apache.jena.graph.Triple;
import org.apache.*;
/**
 * A mapper for counting node usages within triples designed primarily for use
 * in conjunction with {@link NodeCountReducer}
 *
 * @param <TKey> Key type
 */
public class TripleNodeCountMapper<TKey> extends AbstractNodeTupleNodeCountMapper<TKey, Triple, TripleWritable> {

    @Override
    protected NodeWritable[] getNodes(TripleWritable tuple) {
        Triple t = tuple.get();
        return new NodeWritable[] { new NodeWritable(t.getSubject()), 
                                    new NodeWritable(t.getPredicate()),
                                    new NodeWritable(t.getObject()) };
    }
}