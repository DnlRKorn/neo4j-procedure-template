package promiscuity;


import org.neo4j.graphdb.Node;

public class PathEntry extends Entry {
    // These entries contain information we need about nodes stashed on our priority queue.
    public final PathEntry parent;

    public PathEntry(int degree, int path_score, int depth, Node node, PathEntry parent) {
        super(degree, path_score, depth, node);
        this.parent = parent;
    }
}