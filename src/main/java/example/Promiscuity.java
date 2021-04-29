package example;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.*;
import java.util.stream.Stream;

//import Comparator.comparingInt;

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

//* @param k length of paths. If k=2 s->v1->v2->t would be a valid path.
//* @param k length of paths. If k=2 s->v1->v2->t would be a valid path.
    /**
     * This procedure takes a Node and gets the relationships going in and out of it
     * @param sourceNode  node to start promiscuity search from
     * @param tailNode node to end promiscuity search at

     * @return  A RelationshipTypes instance with the relations (incoming and outgoing) for a given node.
     */
    @Procedure(value = "example.promiscuityPath")
    @Description("Get the different relationships going in and out of a node.")
    public Stream<Output> promiscuityPath(
            @Name("sourceNode") Node sourceNode,
            @Name("tailNode") Node tailNode) {
        int k = 2;
        PriorityQueue<Entry> priorityQueue = new PriorityQueue<>();

       sourceNode.getRelationships().iterator()
                .forEachRemaining(rel -> AddToQueue(priorityQueue,rel.getOtherNode(sourceNode),0,1));

       int best_score = Integer.MAX_VALUE;
       while(!priorityQueue.isEmpty()){
           Entry head = priorityQueue.poll();
           Node node = head.node;
           if(head.degree >= best_score){
               return Stream.of(new Output(best_score));
           }
           if(head.depth<=1) {
               node.getRelationships().iterator()
                       .forEachRemaining(rel -> AddToQueue(priorityQueue, rel.getOtherNode(node), 0, head.depth + 1));
           }

       }

        return new ArrayList<Output>().stream();
    }

    /**
     * Prints degree of given node to console
     * @param node the node which we should print degree of
     */
    private void printDegree(Node node){
        System.out.println(node.getDegree());
    }

    /**
     * Creates entry for given node and appends to priority queue.
     * @param priorityQueue  the queue which we are adding the Entry to.
     * @param node the node which we should create Entry for and append to queue.
     */
    private void AddToQueue(PriorityQueue<Entry> priorityQueue, Node node, int path_score, int depth ){
        Entry e = new Entry(node.getDegree(), path_score, depth,node);
        priorityQueue.add(e);
    }

    public static class Output {
        public Number promiscuity_score;

        public Output(Number promiscuity_score) {
            this.promiscuity_score = promiscuity_score;
        }

        public Output(int promiscuity_score) {
            this.promiscuity_score = (Number) promiscuity_score;
        }
    }

    public static class Entry implements Comparator<Entry>, Comparable<Entry>{
        // These entries contain information we need about nodes stashed on our priority queue.
        public int degree;
        public int path_score;
        public int depth;
        public Node node;


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
