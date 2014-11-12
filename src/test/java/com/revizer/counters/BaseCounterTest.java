package com.revizer.counters;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * Created by alanl on 11/12/14.
 */
public class BaseCounterTest {

    protected static ObjectMapper mapper = new ObjectMapper();

    public String createEvent(long now, String aff, String subaff, String country, String browser, String[] revmods)
    {
        String event = "{\"redirect-to\":null,\"classification\":\"\",\"first_encounter\":false,\"language\":\"ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3\",\"x-forwarded-for\":\"37.29.88.100\",\"born\":1412208077,\"host\":\"log.hoodsonline.com\",\"client_data\":{\"addonname\":\"SearchSnacks\",\"from_combo\":null,\"reto\":null,\"mode\":null,\"subaffid\":\"1003\",\"clientid\":null,\"clientuid\":\"undefined\",\"version\":null,\"affid\":\"{AFFID}\",\"publisher_subid\":null},\"referer\":\"http://www.pornotube.name/?vs=\",\"country_code\":\"{COUNTRY}\",\"affid\":{AFFID},\"guid\":\"551459d1-096b-4cab-97b6-31576999801f\",\"previous\":1413244718,\"tc_used\":false,\"user_agent\":\"mozilla/5.0 (windows nt 6.1; rv:32.0) gecko/20100101 firefox/32.0\",\"now\":{NOW},\"subaffid\":{SUBAFF},\"referer_scheme\":\"http\",\"user_encoding\":\"gzip, deflate\",\"browser_major_ver\":\"32\",\"second_encounter\":false,\"age\":1036722,\"browser_ver\":\"32.0\",\"server\":\"rs107-slam\",\"revmods\":[{REVMODS}],\"revmods_ids\":[103,29,6,40],\"version\":[1,0],\"mode\":\"inject\",\"browser\":\"{BROWSER}\",\"referer_domain\":\"www.pornotube.name\",\"opt_out\":[],\"remote_ip\":\"37.29.88.100\",\"euid\":\"80a63851706743af8cddc98604625b22\"}";
        event = event.replace("{NOW}",String.valueOf(now));
        event = event.replace("{AFFID}", aff);
        event = event.replace("{SUBAFF}",subaff);
        event = event.replace("{COUNTRY}",country);
        event = event.replace("{BROWSER}",browser);
        String revmodsOutput = "";
        for (String revmod : revmods) {
            if (!revmodsOutput.equals("")){
                revmodsOutput += ",";
            }
            revmodsOutput += "\"" + revmod + "\"";
        }
        event = event.replace("{REVMODS}",revmodsOutput);
        return event;
    }

}
