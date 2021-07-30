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

        result.add(new OutputQueueCount(best_score,queue_count));

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


    static int dfs_queue_cnt;
    /**
     * This procedure takes a source Node, a tail Node, and a length parameter k. This version of the algorithm is a
     * modification of DFS. It uses a priority queue to ensure only the nodes of low degree are inspected, but does this
     * on a single node. It can be viewed as a modification of "Branch and Prune" methods.
     * It's worst case runtime is O(p * b^(k-1)) and it's memory usage is O(k*b + p)
     *
     * @param sourceNode node to start promiscuity search from
     * @param tailNode   node to end promiscuity search at
     * @param k_input    length of paths. If k=2 s->v1->v2->t would be a valid path. Must be of type Number to satisfy Neo4j.
     * @return An Output instance with the lowest scoring promiscuity score fo the pathway.
     */
    @Procedure(value = "promiscuityQueueCount.promiscuityDFSScoreQueueCount")
    @Description("Get the lowest promiscuity score of paths of length k connecting a source and tail node.")
    public Stream<OutputQueueCount> DFSPromiscuityScore(
            @Name("sourceNode") Node sourceNode,
            @Name("tailNode") Node tailNode,
            @Name("k") Number k_input) {
        ArrayList<OutputQueueCount> result = new ArrayList<>();
        int k = k_input.intValue();
        dfs_queue_cnt = 0;
        PriorityQueue<Entry> priorityQueue = new PriorityQueue<>();

        sourceNode.getRelationships().iterator()
                .forEachRemaining(rel -> AddToQueue(priorityQueue, rel.getOtherNode(sourceNode), 0, 1));

        int best_score = Integer.MAX_VALUE;
        while (!priorityQueue.isEmpty()) {
            dfs_queue_cnt++;
            Entry head = priorityQueue.poll();
            Node node = head.node;
            if (head.degree >= best_score) {
                break;
            }
            int head_score = promiscuity_DFS_routine(node, tailNode,1, k, best_score);
            if (head_score != -1) {
                best_score = min(best_score, head_score);
            }
        }

        result.add(new OutputQueueCount(best_score,dfs_queue_cnt));

        return result.stream();
    }

    /**
     * This procedure serves to explore the provided Node in a recursive DFS style, finding the optimal path from the
     * provided node to a tail node.
     * If(depth==k), check if the node has an edge connected to the tail.
     *      If yes: return the degree of the node.
     *      If no: return -1.
     * If(depth<k), check all neighbors of the node  in recursive fashion.
     *     for neighbor of node:
     *         check if path exists between node and t.
     *             if yes, add score to local minimum and keep searching.
     *     if path_found return MIN(DEGREE(node), path_score)
     *     else: return -1
     **/
    private int promiscuity_DFS_routine(Node node, Node tailNode, int depth, int k,  int best_score) {
        //String name = (String) node.getProperty("name");
        if (depth == k) {
            boolean tail_neighbor = StreamSupport.stream(node.getRelationships().spliterator(), false)
                    .anyMatch(rel -> rel.getOtherNode(node).equals(tailNode));
            if (tail_neighbor) return node.getDegree();
            else return -1;
        }

        PriorityQueue<Entry> priorityQueue = new PriorityQueue<>();

        //We cannot use the promiscuity_subroutine to enqueue the neighbors of the source node, as the degree of the
        // source node has no effect on the promiscuity score of paths.
        node.getRelationships().iterator()
                .forEachRemaining(rel -> AddToQueue(priorityQueue, rel.getOtherNode(node), 0, 1));

        int best_score_local = Integer.MAX_VALUE;

        while (!priorityQueue.isEmpty()) {
            dfs_queue_cnt++;
            Entry head = priorityQueue.poll();
            Node headNode = head.node;
            if (head.degree >= best_score || head.degree >= best_score_local) {
                return max(best_score_local, node.getDegree());
            }
            int head_score = promiscuity_DFS_routine(headNode, tailNode, depth + 1, k, min(best_score, best_score_local));
            if (head_score != -1) {
                best_score_local = min(best_score_local, head_score);
            }
        }

        //best_score_local was never updated. That means we could not find a path from this node to tail (with
        // promiscuity lower than best_score).
        if(best_score_local==Integer.MAX_VALUE){
            return -1;
        }

        return max(best_score_local, node.getDegree());
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

    /**
     * This procedure takes a source Node, a tail Node, and a length parameter k. This version of the algorithm is a
     * modification of DFS. It uses a priority queue to ensure only the nodes of low degree are inspected, but does this
     * on a single node. It can be viewed as a modification of "Branch and Prune" methods.
     * It's worst case runtime is O(p * b^(k-1)) and it's memory usage is O(k*b + p)
     *
     * @param sourceNode node to start promiscuity search from
     * @param tailNode   node to end promiscuity search at
     * @param k_input    length of paths. If k=2 s->v1->v2->t would be a valid path. Must be of type Number to satisfy Neo4j.
     * @return An Output instance with the lowest scoring promiscuity score fo the pathway.
     */
    @Procedure(value = "promiscuityQueueCount.naivePromiscuityDFSScoreQueueCount")
    @Description("Get the lowest promiscuity score of paths of length k connecting a source and tail node.")
    public Stream<OutputQueueCount> naiveDFSPromiscuityScore(
            @Name("sourceNode") Node sourceNode,
            @Name("tailNode") Node tailNode,
            @Name("k") Number k_input) {
        ArrayList<OutputQueueCount> result = new ArrayList<>();
        int k = k_input.intValue();
        Queue<Entry> queue = new LinkedList<>();

        sourceNode.getRelationships().iterator()
                .forEachRemaining(rel -> AddToQueue(queue, rel.getOtherNode(sourceNode), 0, 1));
        dfs_queue_cnt=0;
        int best_score = Integer.MAX_VALUE;
        while (!queue.isEmpty()) {
            dfs_queue_cnt++;
            Entry head = queue.poll();
            Node node = head.node;
            int head_score = promiscuity_naive_DFS_routine(node, tailNode,1, k, best_score);
            if (head_score != -1) {
                best_score = min(best_score, head_score);
            }
        }

        result.add(new OutputQueueCount(best_score,dfs_queue_cnt));

        return result.stream();
    }

    /**
     * This procedure serves to explore the provided Node in a recursive DFS style, finding the optimal path from the
     * provided node to a tail node.
     * If(depth==k), check if the node has an edge connected to the tail.
     *      If yes: return the degree of the node.
     *      If no: return -1.
     * If(depth<k), check all neighbors of the node  in recursive fashion.
     *     for neighbor of node:
     *         check if path exists between node and t.
     *             if yes, add score to local minimum and keep searching.
     *     if path_found return MIN(DEGREE(node), path_score)
     *     else: return -1
     **/
    private int promiscuity_naive_DFS_routine(Node node, Node tailNode, int depth, int k,  int best_score) {
        //String name = (String) node.getProperty("name");
        if(depth==k){
            boolean tail_neighbor = StreamSupport.stream(node.getRelationships().spliterator(), false)
                    .anyMatch(rel -> rel.getOtherNode(node).equals(tailNode));
            if(tail_neighbor) return node.getDegree();
            else return -1;
        }

        Queue<Entry> queue = new LinkedList<>();

        //We cannot use the promiscuity_subroutine to enqueue the neighbors of the source node, as the degree of the
        // source node has no effect on the promiscuity score of paths.
        node.getRelationships().iterator()
                .forEachRemaining(rel -> AddToQueue(queue, rel.getOtherNode(node), 0, 1));

        int best_score_local = Integer.MAX_VALUE;

        while (!queue.isEmpty()) {
            dfs_queue_cnt++;
            Entry head = queue.poll();
            Node headNode = head.node;
            int head_score = promiscuity_naive_DFS_routine(headNode, tailNode,depth+1, k,  min(best_score,best_score_local));
            if (head_score != -1) {
                best_score_local = min(best_score_local, head_score);
            }
        }

        //best_score_local was never updated. That means we could not find a path from this node to tail (with
        // promiscuity lower than best_score).
        if(best_score_local==Integer.MAX_VALUE){
            return -1;
        }

        return max(best_score_local, node.getDegree());
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
