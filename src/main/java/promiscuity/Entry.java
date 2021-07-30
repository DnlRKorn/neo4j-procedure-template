package promiscuity;

import org.neo4j.graphdb.Node;

import java.util.Comparator;

public class Entry implements Comparator<Entry>, Comparable<Entry> {
    // These entries contain information we need about nodes stashed on our priority queue.
    public final int degree;
    public final int path_score;
    public final int depth;
    public final Node node;

    public Entry(int degree, int path_score, int depth, Node node) {
        this.degree = degree;
        this.path_score = path_score;
        this.depth = depth;
        this.node = node;
    }

    @java.lang.Override
    public int compareTo(Entry o) {
        return this.degree - o.degree;
    }

    @java.lang.Override
    public int compare(Entry e1, Entry e2) {
        return e1.degree - e2.degree;
    }

}
