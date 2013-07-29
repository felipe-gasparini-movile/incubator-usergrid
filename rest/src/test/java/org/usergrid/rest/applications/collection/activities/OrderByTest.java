package org.usergrid.rest.applications.collection.activities;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;
import org.usergrid.rest.RestContextTest;
import org.usergrid.rest.test.resource.CustomCollection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.usergrid.utils.MapUtils.hashMap;

/**
 * // TODO: Document this
 * 
 * @author ApigeeCorporation
 * @since 4.0
 */
public class OrderByTest extends RestContextTest {

  @Test
  // USERGRID-1400
  public void orderByShouldNotAffectResults() {

    CustomCollection activities = collection("activities");

    long created = 0;
    Map actor = hashMap("displayName", "Erin");
    Map props = new HashMap();
    props.put("actor", actor);
    props.put("verb", "go");
    props.put("content", "bragh");
    for (int i = 0; i < 20; i++) {
      props.put("ordinal", i);
      JsonNode activity = activities.create(props);
      if (i == 5) {
        created = activity.findValue("created").getLongValue();
      }
    }

    String query = "select * where created > " + created;
    JsonNode node = activities.withQuery(query).get();
    assertEquals(10, node.get("entities").size());

    query = query + " order by created desc";
    node = activities.withQuery(query).get();
    assertEquals(10, node.get("entities").size());
  }

  @Test
  // USERGRID-1520
  public void orderByComesBeforeLimitResult() {

    CustomCollection activities = collection("activities");

    long created = 0;
    Map actor = hashMap("displayName", "Erin");
    Map props = new HashMap();
    int checkResultsNum = 0;

    props.put("actor", actor);
    props.put("verb", "go");
    props.put("content", "bragh");

    for (int i = 0; i < 20; i++) {
      props.put("ordinal", i);
      JsonNode activity = activities.create(props);
      if (i == 0) {
        created = activity.findValue("created").getLongValue();
      }
    }

    String query = "select * where created > " + 1 + " order by created desc";
    String errorQuery = query;

    JsonNode node = activities.withQuery(query).get();// activities.query(query);
    JsonNode incorrectNode = activities.withQuery(errorQuery).withLimit(5).get();

    assertEquals(5, incorrectNode.get("entities").size()); // asserts that limit
                                                           // works

    while (checkResultsNum < 5)
    {
      assertEquals(activities.entityIndex(query, checkResultsNum),
          activities.entityIndexLimit(errorQuery, 5, checkResultsNum));
      // assertEquals(node.get("entities").get(checkResultsNum),incorrectNode.get("entities").get(checkResultsNum));
      checkResultsNum++;
    }
  }

  /*
   * public JsonNode entityIndex(JsonNode container, int index) { return
   * container.get("entities").get(index); }
   */

  @Test
  // USERGRID-1521
  public void orderByReturnCorrectResults() {

    CustomCollection activities = collection("activities");

    int size = 200;

    Map<String, String> actor = hashMap("displayName", "Erin");
    Map<String, Object> props = new HashMap<String, Object>();

    props.put("actor", actor);
    props.put("verb", "go");
    props.put("content", "bragh");

    List<JsonNode> activites = new ArrayList<JsonNode>(size);

    for (int i = 0; i < size; i++) {
      props.put("ordinal", i);
      JsonNode activity = activities.create(props).get("entities").get(0);
      activites.add(activity);
    }

    long lastCreated = activites.get(activites.size() - 1).get("created").asLong();

    String errorQuery = String.format("select * where created <= %d order by created desc", lastCreated);
    String cursor = null;
    int index = size-1;

    do {
      JsonNode response = activities.withQuery(errorQuery).get();
      JsonNode cursorNode = response.get("cursor");
      
      
      cursor = cursorNode != null ? cursorNode.asText() : null;
      
      JsonNode entities = response.get("entities");
      
      int returnSize = entities.size();
      
      for(int i = 0; i < returnSize; i ++, index--){
        assertEquals(activites.get(index), entities.get(i));
      }
      
      activities = activities.withCursor(cursor);

    } while (cursor != null && cursor.length() > 0);
    
    assertEquals("Paged to last result", -1, index);
  }
}
