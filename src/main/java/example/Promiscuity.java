package example;

import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Math.max;
import static java.lang.Math.min;


/**
 * This is an example showing how you could expose Neo4j's full text indexes as
 * two procedures - one for updating indexes, and one for querying by label and
 * the lucene query language.
 */
public class Promiscuity {
    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    /**
     * This procedure takes a Node and gets the relationships going in and out of it
     *
     * @param sourceNode node to start promiscuity search from
     * @param tailNode   node to end promiscuity search at
     * @param k_input    length of paths. If k=2 s->v1->v2->t would be a valid path. Must be of type Number to satisfy Neo4j.
     * @return A RelationshipTypes instance with the relations (incoming and outgoing) for a given node.
     */
    @Procedure(value = "promiscuity.promiscuityScore")
    @Description("Get the lowest promiscuity score of paths of length k connecting a source and tail node.")
    public Stream<Output> promiscuityScore(
            @Name("sourceNode") Node sourceNode,
            @Name("tailNode") Node tailNode,
            @Name("k") Number k_input) {
        ArrayList<Output> result = new ArrayList<>();
        int k = k_input.intValue();
        PriorityQueue<Entry> priorityQueue = new PriorityQueue<>();

        //We cannot use the promiscuity_subroutine to enqueue the neighbors of the source node, as the degree of the
        // source node has no effect on the promiscuity score of paths.
        sourceNode.getRelationships().iterator()
                .forEachRemaining(rel -> AddToQueue(priorityQueue, rel.getOtherNode(sourceNode), 0, 1));

        int best_score = Integer.MAX_VALUE;
        while (!priorityQueue.isEmpty()) {
            Entry head = priorityQueue.poll();
            Node node = head.node;
            if (head.degree >= best_score) {
                result.add(new Output(best_score));
                break;
            }
            int x = promiscuity_subroutine(node, tailNode, head.depth, k, head.path_score, priorityQueue);
            if (x != -1) {
                best_score = min(best_score, x);
            }
        }

        return result.stream();
    }

    private int promiscuity_subroutine(Node node, Node tail, int depth, int k, int path_score, PriorityQueue<Entry> priorityQueue) {
        int updated_path_score = max(node.getDegree(), path_score);
        if (depth == k) {
            boolean tail_neighbor = StreamSupport.stream(node.getRelationships().spliterator(), false)
                    .anyMatch(rel -> rel.getOtherNode(node).equals(tail));
            if (tail_neighbor) return updated_path_score;
            else return -1;
        } else {
            node.getRelationships().iterator()
                    .forEachRemaining(rel -> AddToQueue(priorityQueue, rel.getOtherNode(node), updated_path_score, depth + 1));
        }

        return -1;
    }

    /**
     * Creates entry for given node and appends to priority queue.
     *
     * @param priorityQueue the queue which we are adding the Entry to.
     * @param node          the node which we should create Entry for and append to queue.
     */
    private void AddToQueue(PriorityQueue<Entry> priorityQueue, Node node, int path_score, int depth) {
        Entry e = new Entry(node.getDegree(), path_score, depth, node);
        priorityQueue.add(e);
    }

    public static class Output {
        public final Number promiscuity_score;

        public Output(Number promiscuity_score) {
            this.promiscuity_score = promiscuity_score;
        }

        public Output(int promiscuity_score) {
            this( (Number)promiscuity_score );
        }
    }

    public static class Entry implements Comparator<Entry>, Comparable<Entry> {
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

}
