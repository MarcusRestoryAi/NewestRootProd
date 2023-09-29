import marcus.kafka.payload.User;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

import org.example.Main;

import java.util.ArrayList;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaTests {

    private static User user;
    private static JSONObject myObj;

    @BeforeAll
    static void beforeAll() {
        user = new User();
        user.setFirstName("NewestTestFirst");
        user.setLastName("NewestTestLast");
        user.setId(250L);

        myObj = new JSONObject();
        myObj.put("id", user.getId());
        myObj.put("firstName", user.getFirstName());
        myObj.put("lastName", user.getLastName());
    }

    @Test
    @Order(1)
    public void sendToWebAPITest() {
        //Anropa metod för att skcika den
        String resp = Main.sendToWebAPI(myObj);

        //Jämföra response-värden
        assertEquals(resp, "Json Message send to Kafka Topic");
    }

    @Test
    @Order(2)
    public void getDataFromKafkaTest() {
        //Anropa metod för att hämta Users
        ArrayList<User> users = Main.getDataFromKafka("javaJsonGuides");
        User testUser = users.get(users.size() - 1);

        assertEquals( testUser.getFirstName() , user.getFirstName());
        assertEquals( testUser.getLastName() , user.getLastName());
        assertEquals( testUser.getId() , user.getId());
    }


}
