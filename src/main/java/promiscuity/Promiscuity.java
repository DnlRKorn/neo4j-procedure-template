package promiscuity;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import org.neo4j.graphalgo.impl.util.PathImpl;
import static org.neo4j.graphalgo.impl.util.PathImpl.Builder;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Math.max;
import static java.lang.Math.min;


/**
 * The procedures here aim to provide fast and efficient methods for finding low promiscuity scores and associated
 * pathways within Neo4J graphs. We provide three procedures: promiscuity.promiscuityScore an implementation of our novel
 * promiscuity score calculation algorithm, promiscuity.naivePromiscuityScore an implementation of a promiscuity search
 * which leverages BFS and iterates through all paths in the graph, and promiscuity.promiscuityPath a modification of
 * our novel algorithm which can also return the top-n least promiscuous paths.
 */
public class Promiscuity {
    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    /**
     * This procedure takes a source Node, a tail Node, and a length parameter k. This version of the algorithm is a
     * modification of BFS. It uses a priority queue to ensure only the nodes of low degree are inspected.
     * It's runtime and memory usage are both O(b * p^(k-1)).
     *
     * @param sourceNode node to start promiscuity search from
     * @param tailNode   node to end promiscuity search at
     * @param k_input    length of paths. If k=2 s->v1->v2->t would be a valid path. Must be of type Number to satisfy Neo4j.
     * @return An Output instance with the lowest scoring promiscuity score fo the pathway.
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
            //if (head.degree >= best_score) {
            //    result.add(new Output(best_score));
            //    break;
            //}
            int x = promiscuityScore_subroutine(node, tailNode, head.depth, k, head.path_score, priorityQueue);
            if (x != -1) {
                best_score = min(best_score, x);
                break;
            }
        }
        if(best_score < Integer.MAX_VALUE){
            result.add(new Output(best_score));
        }
        return result.stream();
    }
    /*
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
                break;
            }
            int head_score = promiscuity_DFS_routine(node, 1, k, tailNode, best_score);
            if (head_score != -1) {
                best_score = min(best_score, head_score);
            }
        }
        if(best_score < Integer.MAX_VALUE){
            result.add(new Output(best_score));
        }
        return result.stream();
    }*/




    /**
     * This procedure takes a source Node, a tail Node, and a length parameter k. It computes the optimal top n least
     * promiscuous paths between s and t. In the worst case it's runtime is O(b * p^(k-1)) and it's memory usage is
     * O(k * b * p^(k-1)).
     *
     * @param sourceNode node to start promiscuity search from
     * @param tailNode   node to end promiscuity search at
     * @param k_input    length of paths. If k=2 s->v1->v2->t would be a valid path. Must be of type Number to satisfy Neo4j.
     * @return An Output instance with the lowest scoring promiscuity score fo the pathway.
     */
    @Procedure(value = "promiscuity.promiscuityPath")
    @Description("Get the lowest promiscuity score of paths of length k connecting a source and tail node.")
    public Stream<PathOutput> promiscuityPath(
            @Name("sourceNode") Node sourceNode,
            @Name("tailNode") Node tailNode,
            @Name("k") Number k_input,
            @Name("numPaths") Number numPaths_input) {
        int k = k_input.intValue();
        int numPaths = numPaths_input.intValue();
        ArrayList<PathOutput> results = new ArrayList<>(numPaths*2+5);
        PathOutput emptyResult = new PathOutput(Integer.MAX_VALUE, null);

        for(int i=0;i<numPaths;i++) results.add(emptyResult);

        PriorityQueue<PathEntry> priorityQueue = new PriorityQueue<>();

        //We cannot use the promiscuity_subroutine to enqueue the neighbors of the source node, as the degree of the
        // source node has no effect on the promiscuity score of paths.
        PathEntry sourceNodeEntry = new PathEntry(0,0,0, sourceNode,null);
        sourceNode.getRelationships().iterator()
                .forEachRemaining(rel -> AddToQueue(priorityQueue, rel.getOtherNode(sourceNode), 0, 1,sourceNodeEntry));

        int enqueuedPaths = 0;
        while (!priorityQueue.isEmpty()) {
            int best_score = results.get(numPaths-1).promiscuity_score.intValue();

            PathEntry head = priorityQueue.poll();
            assert head != null;
            if (head.degree >= best_score) break;
            int x = promiscuityPath_subroutine(head, tailNode, k, priorityQueue);
            if (x != -1) {
                Path p = buildPath(head, tailNode);
                results.add(new PathOutput(x, p));
                enqueuedPaths++;
                Collections.sort(results);
            }
        }

        //We only want to return EITHER number of enqueued paths (i.e. if there are only three possible paths and
        // the user requested five, we should return five.) OR the lowest promiscuity score paths up to numPaths.
        int numResults = min(numPaths,enqueuedPaths);

        //Gets the top n (n = numPaths) results from the "results" list. If there are not n paths to return, return
        // every path enqueued.
        return results.subList(0,numResults).stream();
    }

    /**
     * This procedure serves to look a PathEntry object from the top of the queue. We then check if our current depth
     * (length of path) is equal to the desired depth (parameter k). If we are at the desired depth: we check to see if
     * an edge exists between our node and the tail node. If we are not at the desired depth, we add all neighbors of
     * the provided node to the queue, with the depth value (path length) increased by one.
     **/
    int promiscuityPath_subroutine(PathEntry entry, Node tail, int k, PriorityQueue<PathEntry> priorityQueue) {
        Node node = entry.node;
        int updated_path_score = max(node.getDegree(), entry.path_score);
        if (entry.depth == k) {
            boolean tail_neighbor = StreamSupport.stream(node.getRelationships().spliterator(), false)
                    .anyMatch(rel -> rel.getOtherNode(node).equals(tail));
            if (tail_neighbor) return updated_path_score;
            else return -1;
        } else {
            node.getRelationships().iterator()
                    .forEachRemaining(rel -> AddToQueue(priorityQueue, rel.getOtherNode(node), updated_path_score,entry.depth + 1,entry));
        }
        return -1;
    }

    private Path buildPath(PathEntry head, Node tail) {
        ArrayList<Node> nodeArrayList = new ArrayList<>();
        nodeArrayList.add(tail);
        for(PathEntry e = head; e!=null; e=e.parent){
            nodeArrayList.add(e.node);
        }
        Collections.reverse(nodeArrayList);
        Iterator<Node> nodeIterator = nodeArrayList.iterator();
        Node sourceNode = nodeIterator.next();
        PathImpl.Builder builder = new Builder(sourceNode);
        Node currentPathEnd = sourceNode;
        while(nodeIterator.hasNext()){
            Node nextAdditionToPath = nodeIterator.next();
            Relationship rel = getRelationship(currentPathEnd,nextAdditionToPath);
            assert rel != null;
            builder = builder.push(rel);
            currentPathEnd = nextAdditionToPath;
        }
        return builder.build();
    }

    Relationship getRelationship(Node a, Node b){
        for( Relationship rel : a.getRelationships()){
            if(rel.getOtherNode(a).equals(b)) return rel;
        }
        return null;
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
    @Procedure(value = "promiscuity.promiscuityDFSScore")
    @Description("Get the lowest promiscuity score of paths of length k connecting a source and tail node.")
    public Stream<Output> DFSPromiscuityScore(
            @Name("sourceNode") Node sourceNode,
            @Name("tailNode") Node tailNode,
            @Name("k") Number k_input) {
        ArrayList<Output> result = new ArrayList<>();
        int k = k_input.intValue();
        PriorityQueue<Entry> priorityQueue = new PriorityQueue<>();

        sourceNode.getRelationships().iterator()
                .forEachRemaining(rel -> AddToQueue(priorityQueue, rel.getOtherNode(sourceNode), 0, 1));

        int best_score = Integer.MAX_VALUE;
        while (!priorityQueue.isEmpty()) {
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
        if(best_score < Integer.MAX_VALUE){
            result.add(new Output(best_score));
        }
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
        if(depth==k){
            boolean tail_neighbor = StreamSupport.stream(node.getRelationships().spliterator(), false)
                    .anyMatch(rel -> rel.getOtherNode(node).equals(tailNode));
            if(tail_neighbor) return node.getDegree();
            else return -1;
        }

        PriorityQueue<Entry> priorityQueue = new PriorityQueue<>();

        //We cannot use the promiscuity_subroutine to enqueue the neighbors of the source node, as the degree of the
        // source node has no effect on the promiscuity score of paths.
        node.getRelationships().iterator()
                .forEachRemaining(rel -> AddToQueue(priorityQueue, rel.getOtherNode(node), 0, 1));

        int best_score_local = Integer.MAX_VALUE;

        while (!priorityQueue.isEmpty()) {
            Entry head = priorityQueue.poll();
            Node headNode = head.node;
            if (head.degree >= best_score || head.degree >= best_score_local) {
                return max(best_score_local, node.getDegree());
            }
            int head_score = promiscuity_DFS_routine(headNode, tailNode,depth+1, k,  min(best_score,best_score_local));
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
     * This procedure serves to look a node from the top of the queue. We then check if our current depth (length of
     * path) is equal to the desired depth (parameter k). If we are at the desired depth: we check to see if an edge
     * exists between the provided node and the tail node. If we are not at the desired depth, we add all neighbors of
     * the provided node to the queue, with the depth value (path length) increased by one.
     **/
    public int promiscuityScore_subroutine(Node node, Node tail, int depth, int k, int path_score, PriorityQueue<Entry> priorityQueue) {
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
     * This procedure takes a source Node, a tail Node, and a length parameter k. It finds the optimal promiscuity value
     * by traversing all possible paths in the graph.
     *
     * @param sourceNode node to start promiscuity search from
     * @param tailNode   node to end promiscuity search at
     * @param k_input    length of paths. If k=2 s->v1->v2->t would be a valid path. Must be of type Number to satisfy Neo4j.
     * @return An Output instance with the lowest scoring promiscuity score fo the pathway.
     */
    @Procedure(value = "promiscuity.naivePromiscuityScore")
    @Description("Get the lowest promiscuity score of paths of length k connecting a source and tail node.")
    public Stream<Output> naivePromiscuityScore(
            @Name("sourceNode") Node sourceNode,
            @Name("tailNode") Node tailNode,
            @Name("k") Number k_input) {
        ArrayList<Output> result = new ArrayList<>();
        int k = k_input.intValue();
        Queue<Entry> queue = new LinkedList<>();

        //We cannot use the promiscuity_subroutine to enqueue the neighbors of the source node, as the degree of the
        // source node has no effect on the promiscuity score of paths.
        sourceNode.getRelationships().iterator()
                .forEachRemaining(rel -> AddToQueue(queue, rel.getOtherNode(sourceNode), 0, 1));

        int best_score = Integer.MAX_VALUE;
        while (!queue.isEmpty()) {
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
        result.add(new Output(best_score));

        return result.stream();
    }



    /**
     * Creates entry for given node and appends to priority queue.
     *
     * @param priorityQueue the queue which we are adding the Entry to.
     * @param node          the node which we should create Entry for and append to queue.
     */
    void AddToQueue(Queue<Entry> priorityQueue, Node node, int path_score, int depth) {
        Entry e = new Entry(node.getDegree(), path_score, depth, node);
        priorityQueue.add(e);
    }

    /**
     * Creates entry for given node and appends to priority queue.
     *
     * @param priorityQueue the queue which we are adding the Entry to.
     * @param node          the node which we should create Entry for and append to queue.
     */
    void AddToQueue(PriorityQueue<PathEntry> priorityQueue, Node node, int path_score, int depth, PathEntry parent) {
        PathEntry e = new PathEntry(node.getDegree(), path_score, depth, node, parent);
        priorityQueue.add(e);
    }

    public static class Output {
        public final Number promiscuity_score;

        public Output(Number promiscuity_score) {
            this.promiscuity_score = promiscuity_score;
        }

        public Output(int promiscuity_score) {
            this((Number) promiscuity_score);
        }
    }

    public static class PathOutput implements Comparator<PathOutput>, Comparable<PathOutput> {
        public final Number promiscuity_score;
        public final Path promiscuity_path;

        public PathOutput(Number promiscuity_score, Path promiscuity_path) {
            this.promiscuity_score = promiscuity_score;
            this.promiscuity_path = promiscuity_path;
        }

        public PathOutput(int promiscuity_score, Path promiscuity_path) {
            this((Number) promiscuity_score, promiscuity_path);
        }

        @Override
        public int compareTo(PathOutput o) {
            return this.promiscuity_score.intValue() - o.promiscuity_score.intValue();
        }

        @Override
        public int compare(PathOutput p1, PathOutput p2) {
            return p1.promiscuity_score.intValue() - p2.promiscuity_score.intValue();
        }
    }



}
