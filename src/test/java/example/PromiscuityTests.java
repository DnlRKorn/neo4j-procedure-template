package example;

import org.junit.jupiter.api.*;
import org.neo4j.driver.*;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import static org.assertj.core.api.Assertions.assertThat;

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

        this.driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI(), driverConfig);
    }

    @AfterAll
    void closeDriver(){
        this.driver.close();
        this.embeddedDatabaseServer.close();
    }

    @AfterEach
    void cleanDb(){
        try(Session session = driver.session()) {
            session.run("MATCH (n) DETACH DELETE n");
        }
    }

    /**
     * We should only get distinct values from our procedure
     */
    @Test
    public void promiscuityTest() {
        final String expectedIncoming = "INCOMING";
        final String expectedOutgoing = "OUTGOING";

        try(Session session = driver.session()) {
            session.run(String.format("CREATE (:Person)-[:%s]->(:Movie {id:1})<-[:%s]-(:Person)", expectedIncoming, expectedIncoming));
            session.run(String.format("MATCH (m:Movie {id:1}) CREATE (:Person)<-[:%s]-(m)-[:%s]->(:Person)", expectedOutgoing, expectedOutgoing));

            Record record = session.run("MATCH (u:Movie {id:1}) CALL example.promiscuityPath(u) YIELD outgoing, incoming RETURN outgoing, incoming").single();

            assertThat(record.get("incoming").asList(x -> x.asString())).containsOnly();
            assertThat(record.get("outgoing").asList(x -> x.asString())).containsOnly();
        }
    }
}
