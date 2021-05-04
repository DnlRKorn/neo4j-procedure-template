package example;

import org.apache.commons.collections.IteratorUtils;
import org.junit.jupiter.api.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import java.util.ArrayList;
import java.util.Iterator;
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

            Record record = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL " +
                    "promiscuity.promiscuityScore(s,t,1) YIELD promiscuity_score RETURN promiscuity_score").single();
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

//            Record record = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL " +
//                    "promiscuity.promiscuityScore(s,t,1) YIELD promiscuity_score RETURN promiscuity_score").single();
//            assertEquals(record.get("promiscuity_score").asInt(),3);

            //Remove the connection from degree3 and tail. Expect new promiscuity_score of graph to be 5.
            //session.run("MATCH (n:Node {name:'degree3'})-[r:Edge]->(t:Node {name:'tail'}) DELETE r");
            List<Record> record_list = session.run("MATCH (s {name:'source'}), (t {name:'tail'}) CALL promiscuity.promiscuityPath(s,t,1,3) YIELD promiscuity_score, promiscuity_path RETURN promiscuity_score, promiscuity_path").list();
            Record record = record_list.get(0);

            assertEquals(record.get("promiscuity_score").asInt(),3);
            InternalPath p = (InternalPath) record.get("promiscuity_path").asPath();
            Object[] l = IteratorUtils.toArray(p.nodes().iterator());
            Node aa = (Node) l[1];
            assertEquals(aa.getProperty("name"),"degree3");

        //    assertEquals(record.get("promiscuity_score").asInt(),5);
       //     PathValue p = (PathValue) record.get("promiscuity_path");
       //     System.out.println(p.asPath().length());
        }
    }

    private Node getSecondNode(Path p){
        Iterator<Node> nodeIterator = p.nodes().iterator();
        nodeIterator.next();
        return nodeIterator.next();
    }
    /**
     * Tests comparator methods of Entry nodes which we leverage in the priority queue. Should leverage degree as
     * key for comparison.
     */
    @Test
    public void queueEntryTest() {
        final Promiscuity.Entry degree_5 = new Promiscuity.Entry(5,2,3,null);
        final Promiscuity.Entry degree_10 = new Promiscuity.Entry(10,2,3,null);

        final Promiscuity.Entry degree_5_equal_test = new Promiscuity.Entry(5,6,8,null);

        assertTrue(degree_10.compareTo(degree_5) > 0);
        assertTrue(degree_5.compareTo(degree_10) < 0);
        assertEquals(degree_5_equal_test.compareTo(degree_5),0);

    }

    /**
     * Ensure that the comparator method for Entry is functioning for priority queue.
     */
    @Test
    public void priorityQueueTest() {
        final Promiscuity.Entry degree_5 = new Promiscuity.Entry(5,2,3,null);
        final Promiscuity.Entry degree_10 = new Promiscuity.Entry(10,2,3,null);
        final Promiscuity.Entry degree_1 = new Promiscuity.Entry(1,7,5,null);

        PriorityQueue<Promiscuity.Entry> priorityQueue = new PriorityQueue<>();
        priorityQueue.add(degree_5);
        priorityQueue.add(degree_10);
        priorityQueue.add(degree_1);

        Promiscuity.Entry queue_first = priorityQueue.poll();
        Promiscuity.Entry queue_second = priorityQueue.poll();
        Promiscuity.Entry queue_third = priorityQueue.poll();
        Promiscuity.Entry queue_fourth = priorityQueue.poll();

        assertEquals(degree_1,queue_first); //Lowest degree should be first dequeued.
        assertEquals(degree_5,queue_second);
        assertEquals(degree_10,queue_third);
        assertNull(queue_fourth);

    }


}
