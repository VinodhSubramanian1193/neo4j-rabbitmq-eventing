package procedure;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RabbitMQEvent {

    @Context
    public Log log;

    public final static String QUEUE_NAME = "neo4j-create-event";

    @Procedure(value = "rabbitmq.event")
    @Description("Event node details through RabbitMQ")
    public void event(@Name("labels") List<String> labels,
                      @Name("properties") Map<String, Object> properties) {

        log.debug( "Labels: `%s`", labels );
        log.debug( "Properties: `%s`", properties);

        Map<String, Object> message = new HashMap<>();
        message.put("Labels", labels);
        message.put("Properties", properties);
        boolean successEvent = sendEvent(message);
        if(successEvent){
            log.info("Event successfully send to queue");
        }
    }

    public boolean sendEvent(Map<String, Object> message) {
        String payload;
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri("amqp://guest:guest@localhost");
            factory.setConnectionTimeout(1000000);
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            payload = convertToJsonString(message);
            channel.basicPublish("", QUEUE_NAME, null, payload.getBytes());
            connection.close();
        } catch (Exception e) {
            log.error("Error in eventing out"+ e.getMessage());
            return false;
        }
        log.info("Published message : " + payload);
        return true;
    }

    private <T> String convertToJsonString(T model) {
        ObjectWriter ow = new ObjectMapper().writer();
        String expressionText = null;
        try {
            expressionText = ow.writeValueAsString(model);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return expressionText;
    }
}
