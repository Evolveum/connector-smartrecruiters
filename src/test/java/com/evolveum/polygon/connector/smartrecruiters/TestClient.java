/**
 * Copyright (c) 2016 Evolveum
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.evolveum.polygon.connector.smartrecruiters;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.objects.*;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @author gpalos
 */
public class TestClient {

    private static final Log LOG = Log.getLog(TestClient.class);

    private static SmartRecruitersConfiguration conf;
    private static SmartRecruitersConnector conn;

    ObjectClass accountObjectClass = new ObjectClass(ObjectClass.ACCOUNT_NAME);

    @BeforeClass
    public static void setUp() throws Exception {
        String fileName = "test.properties";

        final Properties properties = new Properties();
        InputStream inputStream = TestClient.class.getClassLoader().getResourceAsStream(fileName);
        if (inputStream == null) {
            throw new IOException("Sorry, unable to find " + fileName);
        }
        properties.load(inputStream);

        conf = new SmartRecruitersConfiguration();
        conf.setTokenName(properties.getProperty("tokenName"));
        conf.setTokenValue(new GuardedString(properties.getProperty("tokenValue").toCharArray()));
        conf.setServiceAddress(properties.getProperty("serviceAddress"));
        conf.setAuthMethod(properties.getProperty("authMethod"));
        conf.setTrustAllCertificates(Boolean.parseBoolean(properties.getProperty("trustAllCertificates")));
        conf.setPageSize(Integer.parseInt(properties.getProperty("pageSize")));
        conf.setReadLocation(Boolean.parseBoolean(properties.getProperty("readLocation")));

        conn = new SmartRecruitersConnector();
        conn.init(conf);
    }

    @Test
    public void testConn() {
        conn.test();
    }

    @Test
    public void testSchema() {
        Schema schema = conn.schema();
        LOG.info("schema: " + schema);
    }

    private static String testName = "gustav.palos+sr8"; // TODO increate when you need to test

    @Test
    public void testCreateUser() {

        //create
        Set<Attribute> attributes = new HashSet<Attribute>();
//        String randName = "gustav.palos+sr5";// + (new Random()).nextInt();
        String randName = testName;
        String email = randName + "@gmail.com";
        attributes.add(AttributeBuilder.build(Name.NAME, email));
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_THEME, "theme1"));
        String firstName = "first name";
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_FIRST_NAME, firstName));
        String lastName = "last name";
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LAST_NAME, lastName));
        String role = "EMPLOYEE";
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_ROLE, role));
        String externalData = "pavs:123";
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_EXTERNAL_DATA, externalData));
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_SSO_IDENTIFIER, randName));

//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_CITY, "Bratislava"));
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_COUNTRY_COODE, "sk"));
//
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_COUNTRY, "Slovakia")); // read only
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_REGION_CODE, "123"));
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_REGION, "Bratislava 2"));
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_ADDRESS, "Hlavná 12"));
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_POSTAL_CODE, "123 45"));
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_LONGITUDE, "48.1437983"));
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_LATITUDE, "17.1003687"));

//        String[] roles = {"17", "5", "6"};
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_ROLES, roles));
//
        GuardedString gs = new GuardedString("Test123".toCharArray());
        attributes.add(AttributeBuilder.build(OperationalAttributeInfos.PASSWORD.getName(), gs));

        Boolean enabled = true;
        attributes.add(AttributeBuilder.build(OperationalAttributeInfos.ENABLE.getName(), enabled));

        Uid userUid = conn.create(accountObjectClass, attributes, null);
        LOG.ok("New user Uid is: {0}, name: {1}", userUid.getUidValue(), randName);

        ConnectorObject user = findByUid(userUid.getUidValue());

        Assert.assertEquals(user.getAttributeByName(Name.NAME).getValue().get(0), email);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_FIRST_NAME).getValue().get(0), firstName);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_LAST_NAME).getValue().get(0), lastName);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_ROLE).getValue().get(0), role);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_ROLES).getValue().get(0), role);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_EXTERNAL_DATA).getValue().get(0), externalData);
        Assert.assertEquals(user.getAttributeByName(OperationalAttributeInfos.ENABLE.getName()).getValue().get(0), enabled);
    }

    @Test
    public void testCreateConflictedUser() {

        //create
        Set<Attribute> attributes = new HashSet<Attribute>();
        String name = "gustav.palos@evolveum.com";
        attributes.add(AttributeBuilder.build(Name.NAME, name));
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_FIRST_NAME, "first name"));
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LAST_NAME, "last name"));
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_ROLE, "EMPLOYEE"));
        GuardedString gs = new GuardedString("Test123".toCharArray());
        attributes.add(AttributeBuilder.build(OperationalAttributeInfos.PASSWORD.getName(), gs));
        attributes.add(AttributeBuilder.build(OperationalAttributeInfos.ENABLE.getName(), true));

        Uid userUid = conn.create(accountObjectClass, attributes, null);
        LOG.ok("New user Uid is: {0}, name: {1}", userUid.getUidValue(), name);

        Assert.assertTrue(userUid.getUidValue().contains(SmartRecruitersConnector.CONFLICT), "User " + userUid + " is not conflicted");
    }

    @Test
    public void testActivateUser() {

        Uid uid = new Uid("581ac30de4b0c1dd234d4dde");
        //create
        Set<Attribute> attributes = new HashSet<Attribute>();

        attributes.add(AttributeBuilder.build(OperationalAttributeInfos.ENABLE.getName(), true));

        Uid userUid = conn.update(accountObjectClass, uid, attributes, null);
        LOG.ok("User {0} updated", userUid.getUidValue());

        ConnectorObject user = findByUid(userUid.getUidValue());
        Boolean enabled = true;
        Assert.assertEquals(user.getAttributeByName(OperationalAttributeInfos.ENABLE.getName()).getValue().get(0), enabled);
    }

    @Test
    public void testDeactivateUser() {

        Uid uid = new Uid("581c841ae4b0643d0c458ff5");
        //create
        Set<Attribute> attributes = new HashSet<Attribute>();

        attributes.add(AttributeBuilder.build(OperationalAttributeInfos.ENABLE.getName(), false));

        Uid userUid = conn.update(accountObjectClass, uid, attributes, null);
        LOG.ok("User {0} updated", userUid.getUidValue());

        ConnectorObject user = findByUid(userUid.getUidValue());
        Boolean enabled = false;
        Assert.assertEquals(user.getAttributeByName(OperationalAttributeInfos.ENABLE.getName()).getValue().get(0), enabled);
    }


    @Test
    public void testDeleteUser() {
        Uid uid = new Uid("581ac30de4b0c1dd234d4dde");
        conn.delete(accountObjectClass, uid, null);

        ConnectorObject user = findByUid(uid.getUidValue());
        // exists, but disabled
        Assert.assertEquals(user.getAttributeByName(Uid.NAME).getValue().get(0), uid.getUidValue());
        Boolean enabled = false;
        Assert.assertEquals(user.getAttributeByName(OperationalAttributeInfos.ENABLE.getName()).getValue().get(0), enabled);
    }

    @Test
    public void testUpdateUser() {

        Uid uid = new Uid("581ac30de4b0c1dd234d4dde");
        //create
        Set<Attribute> attributes = new HashSet<Attribute>();
//        String randName = "gustav.palos+sr4";// + (new Random()).nextInt();
        String randName = testName + "v2";
        String email = randName + "@gmail.com";
        attributes.add(AttributeBuilder.build(Name.NAME, email));
        String firstName = "first name V3";
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_FIRST_NAME, firstName));
        String lastName = "last name V3";
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LAST_NAME, lastName));
        String externalData = "pavs:987654";
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_EXTERNAL_DATA, externalData));
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_SSO_IDENTIFIER, randName));
        String city = "Brno";
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_CITY, city));
        String countryCode = "cz";
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_COUNTRY_COODE, countryCode));

//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_COUNTRY, "Czech republic")); //read only
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_REGION_CODE, "321")); //TODO: code, maybe only in USA?
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_REGION, "Praha 4")); //read only
        String address = "Hlava 22";
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_ADDRESS, address));
        String postalCode = "987 654";
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_POSTAL_CODE, postalCode));
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_LONGITUDE, "47.1437983")); //TODO: what to set?
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_LOCATION_LATITUDE, "18.1003687"));

        String role = "STANDARD"; // most powerfull
//        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_ROLE, "role"));
        String[] roles = {"EMPLOYEE", "RESTRICTED" /*BASIC*/, "STANDARD"};
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_ROLES, roles));

        GuardedString gs = new GuardedString("Test123V2".toCharArray());
        attributes.add(AttributeBuilder.build(OperationalAttributeInfos.PASSWORD.getName(), gs));

        Uid userUid = conn.update(accountObjectClass, uid, attributes, null);
        LOG.ok("User {0} updated", userUid.getUidValue());

        ConnectorObject user = findByUid(userUid.getUidValue());

        Assert.assertEquals(user.getAttributeByName(Name.NAME).getValue().get(0), email);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_FIRST_NAME).getValue().get(0), firstName);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_LAST_NAME).getValue().get(0), lastName);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_ROLE).getValue().get(0), role);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_ROLES).getValue().get(0), role);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_EXTERNAL_DATA).getValue().get(0), externalData);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_LOCATION_CITY).getValue().get(0), city);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_LOCATION_COUNTRY_COODE).getValue().get(0), countryCode);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_LOCATION_ADDRESS).getValue().get(0), address);
        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_LOCATION_POSTAL_CODE).getValue().get(0), postalCode);

    }

    @Test
    public void testChangeRoles() {

        Uid uid = new Uid("581ac30de4b0c1dd234d4dde");
        //create
        Set<Attribute> attributes = new HashSet<Attribute>();

        String[] roles = {"EMPLOYEE", "RESTRICTED" /*BASIC*/, "STANDARD"};
//        String[] roles = {"EMPLOYEE"};
        attributes.add(AttributeBuilder.build(SmartRecruitersConnector.ATTR_ROLES, roles));

        Uid userUid = conn.update(accountObjectClass, uid, attributes, null);
        LOG.ok("User {0} updated", userUid.getUidValue());

        ConnectorObject user = findByUid(userUid.getUidValue());

        Assert.assertEquals(user.getAttributeByName(SmartRecruitersConnector.ATTR_ROLES).getValue().get(0), "STANDARD");
    }

    @Test
    public void testChangePasswordUser() {

        Uid uid = new Uid("581ac30de4b0c1dd234d4dde");
        //create
        Set<Attribute> attributes = new HashSet<Attribute>();

        GuardedString gs = new GuardedString("Test123V2".toCharArray());
        attributes.add(AttributeBuilder.build(OperationalAttributeInfos.PASSWORD.getName(), gs));

        Uid userUid = conn.update(accountObjectClass, uid, attributes, null);
        LOG.ok("User {0} updated", userUid.getUidValue());

        // TODO test manually new password
    }

    @Test
    public void findByUid() {
        final int[] count = {0};
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                count[0]++;
                return true;
            }
        };

        // searchByUId
        SmartRecruitersFilter searchByUid = new SmartRecruitersFilter();
        searchByUid.byUid = "581ac30de4b0c1dd234d4dde";
        LOG.ok("start finding");
        conn.executeQuery(accountObjectClass, searchByUid, rh, null);
        LOG.ok("end finding");
        Assert.assertTrue(count[0] > 0, "User not found: " + searchByUid.byUid);
    }

    private ConnectorObject findByUid(String uid) {
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return false; // continue
            }
        };

        // searchByUId
        SmartRecruitersFilter searchByUid = new SmartRecruitersFilter();
        searchByUid.byUid = uid;
        LOG.ok("start finding");
        conn.executeQuery(accountObjectClass, searchByUid, handler, null);
        LOG.ok("end finding");

        return found[0];
    }

    @Test
    public void findByMail() {
        final int[] count = {0};
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                count[0]++;
                return true;
            }
        };

        // searchByUId
        SmartRecruitersFilter searchByUid = new SmartRecruitersFilter();
        searchByUid.byEmailAddress = "palos@evolveum.com";
        conn.executeQuery(accountObjectClass, searchByUid, rh, null);

        Assert.assertTrue(count[0] > 0, "User not found: " + searchByUid.byUid);
    }

    @Test
    public void findByMailSpecial() {
        final int[] count = {0};
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                count[0]++;
                return true;
            }
        };

        // searchByUId
        SmartRecruitersFilter searchByUid = new SmartRecruitersFilter();
        searchByUid.byEmailAddress = "egsmartrecruiters+dkrjukovs_evolutiongaming.com@gmail.com";
        conn.executeQuery(accountObjectClass, searchByUid, rh, null);

        Assert.assertTrue(count[0] > 0, "User not found: " + searchByUid.byUid);
    }

    @Test
    public void findAll() {
        final int[] count = {0};
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                count[0]++;
                return true;
            }
        };

        // all
        SmartRecruitersFilter filter = new SmartRecruitersFilter();
        conn.executeQuery(accountObjectClass, filter, rh, null);

        Assert.assertTrue(count[0] > 0, "Users not found");
    }

    @Test
    public void findOnePage() {
        final int[] count = {0};
        ResultsHandler rh = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                LOG.ok("result {0}", connectorObject);
                count[0]++;
                return true;
            }
        };

        // all
        SmartRecruitersFilter filter = new SmartRecruitersFilter();

        Integer pageSize = 10;// options.getPageSize();
        Integer pagedResultsOffset = 1; //options.getPagedResultsOffset();
        Map<String, Object> map = new HashMap<>();
        map.put(OperationOptions.OP_PAGE_SIZE, pageSize);
        map.put(OperationOptions.OP_PAGED_RESULTS_OFFSET, pagedResultsOffset);
        OperationOptions options = new OperationOptions(map);
        conn.executeQuery(accountObjectClass, filter, rh, options);

        Assert.assertTrue(count[0] == pageSize, "Users not found in desired page size: " + count[0] + " / " + pageSize);
    }

}
