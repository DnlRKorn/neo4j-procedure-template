package promiscuity;

import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import promiscuity.Promiscuity.*;


import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Math.max;
import static java.lang.Math.min;


/**
 * The procedures here aim to enable the user to count the number of node dequeues in the naive and novel verions of the
 * promiscuity score algorithm.
 */
public class PromiscuityQueueNodeCount {
    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    /**
     * This procedure takes a source Node, a tail Node, and a length parameter k.
     *
     * @param sourceNode node to start promiscuity search from
     * @param tailNode   node to end promiscuity search at
     * @param k_input    length of paths. If k=2 s->v1->v2->t would be a valid path. Must be of type Number to satisfy Neo4j.
     * @return An Output instance with the lowest scoring promiscuity score fo the pathway.
     */
    @Procedure(value = "promiscuityQueueCount.promiscuityScoreQueueCount")
    @Description("Get the lowest promiscuity score of paths of length k connecting a source and tail node.")
    public Stream<OutputQueueCount> promiscuityScore(
            @Name("sourceNode") Node sourceNode,
            @Name("tailNode") Node tailNode,
            @Name("k") Number k_input) {
        ArrayList<OutputQueueCount> result = new ArrayList<>();
        int k = k_input.intValue();
        PriorityQueue<Entry> priorityQueue = new PriorityQueue<>();

        //We cannot use the promiscuity_subroutine to enqueue the neighbors of the source node, as the degree of the
        // source node has no effect on the promiscuity score of paths.
        sourceNode.getRelationships().iterator()
                .forEachRemaining(rel -> AddToQueue(priorityQueue, rel.getOtherNode(sourceNode), 0, 1));

        int best_score = Integer.MAX_VALUE;
        int queue_count = 0;
        while (!priorityQueue.isEmpty()) {
            queue_count++;
            Entry head = priorityQueue.poll();
            Node node = head.node;
            int x = promiscuityScore_subroutine(node, tailNode, head.depth, k, head.path_score, priorityQueue);
            if (x != -1) {
                best_score = x;
                break;
            }
        }
        if(best_score < Integer.MAX_VALUE){
            result.add(new OutputQueueCount(best_score,queue_count));
        }
        return result.stream();
    }

    int promiscuityScore_subroutine(Node node, Node tail, int depth, int k, int path_score, PriorityQueue<Entry> priorityQueue) {
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
     * This procedure takes a source Node, a tail Node, and a length parameter k.
     *
     * @param sourceNode node to start promiscuity search from
     * @param tailNode   node to end promiscuity search at
     * @param k_input    length of paths. If k=2 s->v1->v2->t would be a valid path. Must be of type Number to satisfy Neo4j.
     * @return An Output instance with the lowest scoring promiscuity score fo the pathway.
     */
    @Procedure(value = "promiscuityQueueCount.naivePromiscuityScoreQueueCount")
//    @Procedure(value = "p.test")
    @Description("Get the lowest promiscuity score of paths of length k connecting a source and tail node.")
    public Stream<OutputQueueCount> naivePromiscuityScore(
            @Name("sourceNode") Node sourceNode,
            @Name("tailNode") Node tailNode,
            @Name("k") Number k_input) {
        ArrayList<OutputQueueCount> result = new ArrayList<>();
        int k = k_input.intValue();
        Queue<Entry> queue = new LinkedList<>();

        //We cannot use the promiscuity_subroutine to enqueue the neighbors of the source node, as the degree of the
        // source node has no effect on the promiscuity score of paths.
        sourceNode.getRelationships().iterator()
                .forEachRemaining(rel -> AddToQueue(queue, rel.getOtherNode(sourceNode), 0, 1));

        int best_score = Integer.MAX_VALUE;
        int queue_count = 0;
        while (!queue.isEmpty()) {
            queue_count++;
            Entry head = queue.poll();
            Node node = head.node;
            int updated_path_score = max(node.getDegree(), head.path_score);
            if (head.depth == k) {
                boolean tail_neighbor = StreamSupport.stream(node.getRelationships().spliterator(), false)
                        .anyMatch(rel -> rel.getOtherNode(node).equals(tailNode));
                if(tail_neighbor) best_score = min(best_score,updated_path_score);
            }
            else{
                node.getRelationships().iterator()
                        .forEachRemaining(rel -> AddToQueue(queue, rel.getOtherNode(node), updated_path_score, head.depth+1));
            }
        }
        result.add(new OutputQueueCount(best_score,queue_count));

        return result.stream();
    }

    void AddToQueue(Queue<Entry> priorityQueue, Node node, int path_score, int depth) {
        Entry e = new Entry(node.getDegree(), path_score, depth, node);
        priorityQueue.add(e);
    }


    public static class OutputQueueCount {
        public final Number promiscuity_score;
        public final Number queue_count;

        public OutputQueueCount(Number promiscuity_score, Number queue_count) {
            this.promiscuity_score = promiscuity_score;
            this.queue_count = queue_count;
        }

        public OutputQueueCount(int promiscuity_score, int queue_count) {
            this( (Number) promiscuity_score, (Number) queue_count);
        }
    }


}
