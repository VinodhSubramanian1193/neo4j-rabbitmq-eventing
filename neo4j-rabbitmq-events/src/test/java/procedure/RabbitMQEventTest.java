package procedure;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.List;
import java.util.Map;

import static org.neo4j.driver.v1.Values.parameters;

public class RabbitMQEventTest {

    @Rule
    public Neo4jRule neo4jRule = new Neo4jRule().withProcedure(RabbitMQEvent.class);

    @Test
    public void shouldAcceptNodeLabelAndPropertiesFromTrigger() {
        try (Driver driver = GraphDatabase.driver(neo4jRule.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {
            StatementResult result = session.run("CREATE (u:User {id: 1, email: \"f...@bar.com\"})-[:IS]->(c:Contact {id:2, first: \"foo\", last: \"bar\"})\n" +
                    "RETURN labels(c), c AS contact");
            Record next = result.next();
            List<Object> labels = next.get(0).asList();
            Map<String, Object> properties = next.get(1).asMap();
            StatementResult statementResult = session.run("CALL rabbitmq.event({labels}, {properties})", parameters("labels", labels, "properties", properties));
        }
    }
}