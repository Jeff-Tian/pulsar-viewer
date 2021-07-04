import org.apache.pulsar.client.api.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SimpleReader {

    private static final String SERVICE_URL = "pulsar+ssl://uswest2.aws.kafkaesque.io:6651";

    public static void main(String[] args) throws IOException {

        // Create client object
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .authentication(
                        AuthenticationFactory.token("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJqZWZmLXRpYW4tb3V0bG9vay1jb20tY2xpZW50LTYwZDk4N2FmMDExNTIifQ.LFsD5IH9PQB-B6xu_N8Ogs8Q0tDYOj4ok8yHNrMNEJDhy3Va3CyAUKtN_pSfwk6EucwVsMDWF3r43ZvzbcJO9kVC4LCYMw4R_c5xxlQ0T1yzIuq4z3JzY_erq8zrPAWnwCWEZKoJZJnLFmTHDESCvKJ1hZPxDoVysBW46V7phoC900JquPKwjJLfQo4aXIXQk-ZVycCde7Sivp-rHP0gbSL_byRxZRfgPOwuW4K_xpllbkH2tKir4lgv8MhoHEGX5W62nkL18FOWhP9E3Jmb-jQ1u_pl2aMK4rhpM0vMcssE-tKS7DUYggZ1FgVAqnNa0cK4syDN68J4BvfnHzFrMA")
                )
                .build();

        // Create a reader on a topic starting at the earliest retained message
        // No subscription is necessary. Depending on retention policy, the
        // earliest message may be days old
        Reader<byte[]> reader = client.newReader()
                .topic("jeff-tian-outlook-com/local-uswest2-aws/test-topic")
                .startMessageId(MessageId.earliest)
                .create();

        boolean receivedMsg = false;
        // Loop until a message is received
        do {
            // Block for up to 1 second for a message
            Message msg = reader.readNext(1, TimeUnit.SECONDS);

            if(msg != null){
                System.out.printf("Message received: %s%n",  new String(msg.getData()));

                receivedMsg = true;
            }

        } while (!receivedMsg);

        //Close the reader
        reader.close();

        // Close the client
        client.close();

    }
}