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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.PriorityQueue;

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


    /**
     * This procedure takes a Node and gets the relationships going in and out of it
     *
     * @param node  The node to get the relationships for
     * @return  A RelationshipTypes instance with the relations (incoming and outgoing) for a given node.
     */
    @Procedure(value = "example.promiscuityPath")
    @Description("Get the different relationships going in and out of a node.")
    public Stream<RelationshipTypes2> promiscuityPath(@Name("node") Node node) {
        List<String> outgoing = new ArrayList<>();
        List<String> incoming = new ArrayList<>();

       node.getRelationships(Direction.OUTGOING).iterator()
                .forEachRemaining(rel -> printDegree(rel.getOtherNode(node)));

//        node.getRelationships(Direction.INCOMING).iterator()
//                .forEachRemaining(rel -> AddDistinct(incoming, rel));

        return Stream.of(new RelationshipTypes2(incoming, outgoing));
    }

    /**
     * Prints degree of given node to console
     *
     * @param node the node which we should print degree of
     */
    private void printDegree(Node node){
        System.out.println(node.getDegree());
    }


    /**
     * This is the output record for our search procedure. All procedures
     * that return results return them as a Stream of Records, where the
     * records are defined like this one - customized to fit what the procedure
     * is returning.
     * <p>
     * These classes can only have public non-final fields, and the fields must
     * be one of the following types:
     *
     * <ul>
     *     <li>{@link String}</li>
     *     <li>{@link Long} or {@code long}</li>
     *     <li>{@link Double} or {@code double}</li>
     *     <li>{@link Number}</li>
     *     <li>{@link Boolean} or {@code boolean}</li>
     *     <li>{@link Node}</li>
     *     <li>{@link Relationship}</li>
     *     <li>{@link Path}</li>
     *     <li>{@link Map} with key {@link String} and value {@link Object}</li>
     *     <li>{@link List} of elements of any valid field type, including {@link List}</li>
     *     <li>{@link Object}, meaning any of the valid field types</li>
     * </ul>
     */
    public static class RelationshipTypes2 {
        // These records contain two lists of distinct relationship types going in and out of a Node.
        public List<String> outgoing;
        public List<String> incoming;

        public RelationshipTypes2(List<String> incoming, List<String> outgoing) {
            this.outgoing = outgoing;
            this.incoming = incoming;
        }
    }

    public static class Entry implements Comparable<Entry>{
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

    }
}
