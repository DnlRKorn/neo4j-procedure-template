package promiscuity;

import org.apache.commons.collections.IteratorUtils;
import org.junit.jupiter.api.*;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.types.Path;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.util.List;
import java.util.PriorityQueue;

import static org.junit.jupiter.api.Assertions.*;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PromiscuityTests {

    private static final Config driverConfig = Config.builder().withoutEncryption().build();
    private static Driver driver;
    private Neo4j embeddedDatabaseServer;

    @BeforeAll
    void initializeNeo4j() {
        this.embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
                .withDisabledServer()
                .withProcedure(Promiscuity.class)
                .withProcedure(PromiscuityQueueNodeCount.class)
                .build();

        driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
    }

    @AfterAll
    void closeDriver(){
        driver.close();
        this.embeddedDatabaseServer.close();
    }

    @AfterEach
    void cleanDb(){
        try(Session session = driver.session()) {
            session.run("MATCH (n) DETACH DELETE n");
        }
    }

    /**
     * Creates a graph with three pathways from a source to a tail node. One pathway through a node with degree 3, one
     * through a node with degree 5, and one through node with degree 10. Runs promiscuity.promiscuityScore and tests
     * output.
     */
    @Test
    public void promiscuityScoreTest() {

        try(Session session = driver.session()) {
            buildTestGraph(session);

            Record record = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL " +
                    "promiscuity.promiscuityScore(s,t,1) YIELD promiscuity_score RETURN promiscuity_score").single();
            assertEquals(record.get("promiscuity_score").asInt(),3);

            //Remove the connection from degree3 and tail. Expect new promiscuity_score of graph to be 5.
            session.run("MATCH (n:Node {name:'degree3'})-[r:Edge]->(t:Node {name:'tail'}) DELETE r");
            record = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL promiscuity.promiscuityScore(s,t,1) YIELD promiscuity_score RETURN promiscuity_score").single();
            assertEquals(record.get("promiscuity_score").asInt(),5);

        }
    }

    /**
     * Creates a graph with three pathways from a source to a tail node. One pathway through a node with degree 3, one
     * through a node with degree 5, and one through node with degree 10. Runs promiscuity.promiscuityScore and tests
     * output.
     */
    @Test
    public void promiscuityDFSScoreTest() {

        try(Session session = driver.session()) {
            buildTestGraph(session);

            Record record = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL " +
                    "promiscuity.promiscuityDFSScore(s,t,1) YIELD promiscuity_score RETURN promiscuity_score").single();
            assertEquals(record.get("promiscuity_score").asInt(),3);

            //Remove the connection from degree3 and tail. Expect new promiscuity_score of graph to be 5.
            session.run("MATCH (n:Node {name:'degree3'})-[r:Edge]->(t:Node {name:'tail'}) DELETE r");
            record = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL promiscuity.promiscuityDFSScore(s,t,1) YIELD promiscuity_score RETURN promiscuity_score").single();
            assertEquals(record.get("promiscuity_score").asInt(),5);

        }
    }

    /**
     * Creates a graph with three pathways from a source to a tail node. One pathway through a node with degree 3, one
     * through a node with degree 5, and one through node with degree 10. Runs promiscuity.naivePromiscuityScore and
     * tests output.
     */
    @Test
    public void naivePromiscuityScoreTest() {

        try(Session session = driver.session()) {
            buildTestGraph(session);

            List<Record> records = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL " +
                    "promiscuity.naivePromiscuityScore(s,t,1) YIELD promiscuity_score RETURN promiscuity_score").list();
            assertEquals(records.size(),1);
            Record record = records.get(0);
            assertEquals(record.get("promiscuity_score").asInt(),3);

            //Remove the connection from degree3 and tail. Expect new promiscuity_score of graph to be 5.
            session.run("MATCH (n:Node {name:'degree3'})-[r:Edge]->(t:Node {name:'tail'}) DELETE r");
            record = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL promiscuity.promiscuityScore(s,t,1) YIELD promiscuity_score RETURN promiscuity_score").single();
            assertEquals(record.get("promiscuity_score").asInt(),5);

        }
    }

    @Test
    public void promiscuityPathTest() {

        try(Session session = driver.session()) {
            buildTestGraph(session);

            List<Record> record_list = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL promiscuity.promiscuityPath(s,t,1,3) YIELD promiscuity_score, promiscuity_path RETURN promiscuity_score, promiscuity_path").list();
            Record record = record_list.get(0);

            assertEquals(record.get("promiscuity_score").asInt(),3);
            Path p = record.get("promiscuity_path").asPath();
            Object[] nodeArray = IteratorUtils.toArray(p.nodes().iterator());
            //Confirm the nodes of the path ( s -> x -> t ) are in the order we expect.
            assertEquals(((InternalNode) nodeArray[0]).get("name").asString(),"source");
            assertEquals(((InternalNode) nodeArray[1]).get("name").asString(),"degree3");
            assertEquals(((InternalNode) nodeArray[2]).get("name").asString(),"tail");
            assertEquals(nodeArray.length,3);

            record = record_list.get(1);

            assertEquals(record.get("promiscuity_score").asInt(),5);
            p = record.get("promiscuity_path").asPath();
            nodeArray = IteratorUtils.toArray(p.nodes().iterator());
            assertEquals(((InternalNode) nodeArray[0]).get("name").asString(),"source");
            assertEquals(((InternalNode) nodeArray[1]).get("name").asString(),"degree5");
            assertEquals(((InternalNode) nodeArray[2]).get("name").asString(),"tail");
            assertEquals(nodeArray.length,3);

            record = record_list.get(2);

            assertEquals(record.get("promiscuity_score").asInt(),10);
            p = record.get("promiscuity_path").asPath();
            nodeArray = IteratorUtils.toArray(p.nodes().iterator());
            assertEquals(((InternalNode) nodeArray[0]).get("name").asString(),"source");
            assertEquals(((InternalNode) nodeArray[1]).get("name").asString(),"degree10");
            assertEquals(((InternalNode) nodeArray[2]).get("name").asString(),"tail");
            assertEquals(nodeArray.length,3);

            assertEquals(record_list.size(),3);

            //There should only be three paths possible to find, even though we request up to 1000.
            record_list = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL promiscuity.promiscuityPath(s,t,1,1000) YIELD promiscuity_score, promiscuity_path RETURN promiscuity_score, promiscuity_path").list();
            assertEquals(record_list.size(),3);

        }
    }

    /*
    This procedure creates a simple graph in our neo4j session. In this graph are a node labeled source, a node labeled
    tail, and three nodes which act as an intermediary path between source and tail (s->x->t). These three nodes have
    differing degrees, indicated by their names. One has degree of 3, one has degree of 5, and one has degree of 10.
     */
    private void buildTestGraph(Session session){
        session.run("CREATE (s:Node {name:'source'})");
        session.run("CREATE (t:Node {name:'tail'})");

        session.run("MATCH (s {name:'source'}) CREATE p=(s)-[r:Edge]->(degree3:Node {name:'degree3'})");
        session.run("MATCH (n:Node {name:'degree3'}), (t:Node {name:'tail'}) CREATE (n)-[r:Edge]->(t)");
        //We want "degree3" to have node.degree() == 3. Has a 2 edges to source and tail, create 3 - 2 additional
        // connections.
        for(int i=0;i<3-2;i++){
            session.run(String.format("MATCH (n:Node {name:'degree3'}) CREATE p=(n)-[r:Edge]->(a:Node {name:'a%d'})",i));
            //System.out.println(session.run("MATCH (n) RETURN COUNT(DISTINCT(n))").single());
        }
        session.run("MATCH (s:Node {name:'source'}) CREATE p=(s)-[r:Edge]->(degree5:Node {name:'degree5'})");
        session.run("MATCH (n:Node {name:'degree5'}), (t:Node {name:'tail'}) CREATE (n)-[r:Edge]->(t)");
        for(int i=0;i<5-2;i++){
            session.run(String.format("MATCH (n:Node {name:'degree5'}) CREATE p=(n)-[r:Edge]->(a:Node {name:'b%d'})",i));
        }
        session.run("MATCH (s:Node {name:'source'})  CREATE p=(s)-[r:Edge]->(degree10:Node {name:'degree10'})");
        session.run("MATCH (n:Node {name:'degree10'}), (t:Node {name:'tail'}) CREATE (n)-[r:Edge]->(t)");
        for(int i=0;i<10-2;i++){
            session.run(String.format("MATCH (n:Node {name:'degree10'}) CREATE p=(n)-[r:Edge]->(a:Node {name:'c%d'})",i));
        }
    }

    @Test
    public void promiscuityPathTest_kEquals2() {

        try(Session session = driver.session()) {
            //Create our test graph. Remove the connection between our existing paths and the tail node. Introduce a new
            // node which links our nodes to the tail node titled "intermediate". The resulting paths will look like
            // s -> n -> i -> t.

            buildTestGraph(session);

            session.run("MATCH (n:Node {name:'degree3'})-[r:Edge]->(t:Node {name:'tail'}) DELETE r");
            session.run("MATCH (n:Node {name:'degree5'})-[r:Edge]->(t:Node {name:'tail'}) DELETE r");
            session.run("MATCH (n:Node {name:'degree10'})-[r:Edge]->(t:Node {name:'tail'}) DELETE r");

            session.run("CREATE (i:Node {name:'intermediate'})");
            session.run("MATCH (i:Node {name:'intermediate'}),(t:Node {name:'tail'}) CREATE (i)-[r:Edge]->(t)");


            session.run("MATCH (n:Node {name:'degree3'}), (i:Node {name:'intermediate'}) CREATE (n)-[r:Edge]->(i)");
            session.run("MATCH (n:Node {name:'degree5'}), (i:Node {name:'intermediate'}) CREATE (n)-[r:Edge]->(i)");
            session.run("MATCH (n:Node {name:'degree10'}), (i:Node {name:'intermediate'}) CREATE (n)-[r:Edge]->(i)");




            List<Record> record_list = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL promiscuity.promiscuityPath(s,t,2,3) YIELD promiscuity_score, promiscuity_path RETURN promiscuity_score, promiscuity_path").list();
            Record record = record_list.get(0);

            assertEquals(record.get("promiscuity_score").asInt(),4);
            Path p = record.get("promiscuity_path").asPath();
            Object[] nodeArray = IteratorUtils.toArray(p.nodes().iterator());
            //Confirm the nodes of the path ( s -> x -> t ) are in the order we expect.
            assertEquals(((InternalNode) nodeArray[0]).get("name").asString(),"source");
            assertEquals(((InternalNode) nodeArray[1]).get("name").asString(),"degree3");
            assertEquals(((InternalNode) nodeArray[2]).get("name").asString(),"intermediate");
            assertEquals(((InternalNode) nodeArray[3]).get("name").asString(),"tail");
            assertEquals(nodeArray.length,4);

            record = record_list.get(1);

            assertEquals(record.get("promiscuity_score").asInt(),5);
            p = record.get("promiscuity_path").asPath();
            nodeArray = IteratorUtils.toArray(p.nodes().iterator());
            assertEquals(((InternalNode) nodeArray[0]).get("name").asString(),"source");
            assertEquals(((InternalNode) nodeArray[1]).get("name").asString(),"degree5");
            assertEquals(((InternalNode) nodeArray[2]).get("name").asString(),"intermediate");
            assertEquals(((InternalNode) nodeArray[3]).get("name").asString(),"tail");
            assertEquals(nodeArray.length,4);

            record = record_list.get(2);

            assertEquals(record.get("promiscuity_score").asInt(),10);
            p = record.get("promiscuity_path").asPath();
            nodeArray = IteratorUtils.toArray(p.nodes().iterator());
            assertEquals(((InternalNode) nodeArray[0]).get("name").asString(),"source");
            assertEquals(((InternalNode) nodeArray[1]).get("name").asString(),"degree10");
            assertEquals(((InternalNode) nodeArray[2]).get("name").asString(),"intermediate");
            assertEquals(((InternalNode) nodeArray[3]).get("name").asString(),"tail");
            assertEquals(nodeArray.length,4);

            assertEquals(record_list.size(),3);

            //There should only be three paths possible to find, even though we request up to 1000.
            record_list = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL promiscuity.promiscuityPath(s,t,2,1000) YIELD promiscuity_score, promiscuity_path RETURN promiscuity_score, promiscuity_path").list();
            assertEquals(record_list.size(),3);


            //Increase the degree of the intermediate node to 15. This should force all paths to have a promiscuity
            // score of 15.
            for(int i=0;i<15-4;i++){
                session.run(String.format("MATCH (i:Node {name:'intermediate'}) CREATE p=(i)-[r:Edge]->(a:Node {name:'d%d'})",i));
            }

            record_list = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL promiscuity.promiscuityPath(s,t,2,3) YIELD promiscuity_score, promiscuity_path RETURN promiscuity_score, promiscuity_path").list();
            for(  Record r  : record_list ) {
                assertEquals(r.get("promiscuity_score").asInt(),15);
            }
        }
    }

    /**
     * Tests comparator methods of Entry nodes which we leverage in the priority queue. Should leverage degree as
     * key for comparison.
     */
    @Test
    public void queueEntryTest() {
        final promiscuity.Entry degree_5 = new promiscuity.Entry(5,2,3,null);
        final promiscuity.Entry degree_10 = new promiscuity.Entry(10,2,3,null);

        final promiscuity.Entry degree_5_equal_test = new promiscuity.Entry(5,6,8,null);

        assertTrue(degree_10.compareTo(degree_5) > 0);
        assertTrue(degree_5.compareTo(degree_10) < 0);
        assertEquals(degree_5_equal_test.compareTo(degree_5),0);

    }

    /**
     * Ensure that the comparator method for Entry is functioning for priority queue.
     */
    @Test
    public void priorityQueueTest() {
        final promiscuity.Entry degree_5 = new promiscuity.Entry(5,2,3,null);
        final promiscuity.Entry degree_10 = new promiscuity.Entry(10,2,3,null);
        final promiscuity.Entry degree_1 = new promiscuity.Entry(1,7,5,null);

        PriorityQueue<promiscuity.Entry> priorityQueue = new PriorityQueue<>();
        priorityQueue.add(degree_5);
        priorityQueue.add(degree_10);
        priorityQueue.add(degree_1);

        promiscuity.Entry queue_first = priorityQueue.poll();
        promiscuity.Entry queue_second = priorityQueue.poll();
        promiscuity.Entry queue_third = priorityQueue.poll();
        promiscuity.Entry queue_fourth = priorityQueue.poll();

        assertEquals(degree_1,queue_first); //Lowest degree should be first dequeued.
        assertEquals(degree_5,queue_second);
        assertEquals(degree_10,queue_third);
        assertNull(queue_fourth);

    }


    @Test
    public void promiscuityQueueCountTest() {

        try(Session session = driver.session()) {
            buildTestGraph(session);

            Record record = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL " +
                    "promiscuityQueueCount.promiscuityScoreQueueCount(s,t,1) YIELD promiscuity_score, queue_count RETURN promiscuity_score, queue_count").single();
            assertEquals(record.get("promiscuity_score").asInt(),3);

        }
    }

    @Test
    public void promiscuityNaiveQueueCountTest() {

        try(Session session = driver.session()) {
            buildTestGraph(session);

            Record record = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL " +
                    "promiscuityQueueCount.naivePromiscuityScoreQueueCount(s,t,1) YIELD promiscuity_score, queue_count RETURN promiscuity_score, queue_count").single();
            assertEquals(record.get("promiscuity_score").asInt(),3);

        }
    }

    @Test
    public void promiscuityDFSTest_kEquals2() {

        try(Session session = driver.session()) {
            //Create our test graph. Remove the connection between our existing paths and the tail node. Introduce a new
            // node which links our nodes to the tail node titled "intermediate". The resulting paths will look like
            // s -> n -> i -> t.

            buildTestGraph(session);

            session.run("MATCH (n:Node {name:'degree3'})-[r:Edge]->(t:Node {name:'tail'}) DELETE r");
            session.run("MATCH (n:Node {name:'degree5'})-[r:Edge]->(t:Node {name:'tail'}) DELETE r");
            session.run("MATCH (n:Node {name:'degree10'})-[r:Edge]->(t:Node {name:'tail'}) DELETE r");

            session.run("CREATE (i:Node {name:'intermediate'})");
            session.run("MATCH (i:Node {name:'intermediate'}),(t:Node {name:'tail'}) CREATE (i)-[r:Edge]->(t)");


            session.run("MATCH (n:Node {name:'degree3'}), (i:Node {name:'intermediate'}) CREATE (n)-[r:Edge]->(i)");
            session.run("MATCH (n:Node {name:'degree5'}), (i:Node {name:'intermediate'}) CREATE (n)-[r:Edge]->(i)");
            session.run("MATCH (n:Node {name:'degree10'}), (i:Node {name:'intermediate'}) CREATE (n)-[r:Edge]->(i)");




            List<Record> record_list = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL promiscuity.promiscuityDFSScore(s,t,2) YIELD promiscuity_score RETURN promiscuity_score").list();
            Record record = record_list.get(0);

            assertEquals(record.get("promiscuity_score").asInt(),4);


            //Increase the degree of the intermediate node to 15. This should force all paths to have a promiscuity
            // score of 15.
            for(int i=0;i<15-4;i++){
                session.run(String.format("MATCH (i:Node {name:'intermediate'}) CREATE p=(i)-[r:Edge]->(a:Node {name:'d%d'})",i));
            }

            record_list = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL promiscuity.promiscuityDFSScore(s,t,2) YIELD promiscuity_score RETURN promiscuity_score").list();
            for(  Record r  : record_list ) {
                assertEquals(r.get("promiscuity_score").asInt(),15);
            }
        }
    }



}
