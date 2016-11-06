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

import com.evolveum.polygon.rest.AbstractRestConnector;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.AlreadyExistsException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.*;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author gpalos
 */
@ConnectorClass(displayNameKey = "smartrecruiters.connector.display", configurationClass = SmartRecruitersConfiguration.class)
public class SmartRecruitersConnector extends AbstractRestConnector<SmartRecruitersConfiguration> implements PoolableConnector, TestOp, SchemaOp, CreateOp, DeleteOp, UpdateOp, SearchOp<SmartRecruitersFilter> {

    private static final Log LOG = Log.getLog(SmartRecruitersConnector.class);

    public static final String ATTR_ID = "id";
    public static final String ATTR_EMAIL = "email"; //icfs:name
    public static final String ATTR_FIRST_NAME = "firstName";
    public static final String ATTR_LAST_NAME = "lastName";
    public static final String ATTR_ACTIVE = "active";
    public static final String ATTR_ROLE = "role"; // role (single value) to SmartRecruiters
    public static final String ATTR_ROLES = "roles"; // roles (multi value) to SmartRecruiters - connetor choose the most stronger from here
    public static final String ATTR_UPDATED_ON = "updatedOn";
    public static final String ATTR_EXTERNAL_DATA = "externalData";
    public static final String ATTR_PASSWORD = "password";
    public static final String ATTR_SSO_IDENTIFIER = "ssoIdentifier"; //TODO how to get & change?

    public static final String ATTR_LOCATION = "location";
    public static final String ATTR_LOCATION_COUNTRY = "country";
    public static final String ATTR_LOCATION_COUNTRY_COODE = "countryCode";
    public static final String ATTR_LOCATION_REGION_CODE = "regionCode";
    public static final String ATTR_LOCATION_REGION = "region";
    public static final String ATTR_LOCATION_CITY = "city";
    public static final String ATTR_LOCATION_ADDRESS = "address";
    public static final String ATTR_LOCATION_POSTAL_CODE = "postalCode";
    public static final String ATTR_LOCATION_LONGITUDE = "longitude";
    public static final String ATTR_LOCATION_LATITUDE = "latitude";

    public static final String ATTR_EMAIL_CONFLICT = "emailConflict"; //true, if found used e-mail address in other company/domain
    public static final String CONFLICT = "CONFLICT"; //detect in UID if this is a conflict
    public static final String CONFLICT_SEPARATOR = "|";

    private static final String[] AVAILABLE_ROLES = {"EMPLOYEE", "RESTRICTED" /*BASIC in manual*/, "STANDARD", "EXTENDED", "ADMINISTRATOR"};

    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String CONTENT_TYPE_JSON_PATCH = "application/json-patch+json";


    @Override
    public void test() {
        LOG.ok("test - reading me");
        try {
            HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + "/me");
            callRequest(request, true);
        } catch (IOException e) {
            throw new ConnectorIOException("Error when testing connection: " + e.getMessage(), e);
        }
    }

    @Override
    public void init(Configuration configuration) {
        super.init(configuration);
        LOG.ok("configuration: {0}", ((SmartRecruitersConfiguration) this.getConfiguration()).toString());
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    @Override
    public Schema schema() {
        SchemaBuilder schemaBuilder = new SchemaBuilder(SmartRecruitersConnector.class);
        schemaBuilder.defineObjectClass(schemaAccount());
        return schemaBuilder.build();
    }

    private ObjectClassInfo schemaAccount() {
        ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();

        // UID, NAME (email), PASSWORD are defaults

        // read only and only when conflict is detected
        AttributeInfoBuilder attrEmailConflictBuilder = new AttributeInfoBuilder(ATTR_EMAIL_CONFLICT, Boolean.class);
        attrEmailConflictBuilder.setReturnedByDefault(false);
        attrEmailConflictBuilder.setCreateable(false);
        attrEmailConflictBuilder.setUpdateable(false);
        attrEmailConflictBuilder.setReadable(true);
        objClassBuilder.addAttributeInfo(attrEmailConflictBuilder.build());

        AttributeInfoBuilder firstNameBuilder = new AttributeInfoBuilder(ATTR_FIRST_NAME);
        objClassBuilder.addAttributeInfo(firstNameBuilder.build());
        AttributeInfoBuilder lastNameBuilder = new AttributeInfoBuilder(ATTR_LAST_NAME);
        objClassBuilder.addAttributeInfo(lastNameBuilder.build());
        AttributeInfoBuilder updateOnBuilder = new AttributeInfoBuilder(ATTR_UPDATED_ON);
        objClassBuilder.addAttributeInfo(updateOnBuilder.build());
        AttributeInfoBuilder roleBuilder = new AttributeInfoBuilder(ATTR_ROLE);
        objClassBuilder.addAttributeInfo(roleBuilder.build());

        AttributeInfoBuilder attrRolesBuilder = new AttributeInfoBuilder(ATTR_ROLES, String.class);
        attrRolesBuilder.setMultiValued(true);
        objClassBuilder.addAttributeInfo(attrRolesBuilder.build());

        AttributeInfoBuilder externalDataBuilder = new AttributeInfoBuilder(ATTR_EXTERNAL_DATA);
        objClassBuilder.addAttributeInfo(externalDataBuilder.build());

        AttributeInfoBuilder ssoIdentifierDataBuilder = new AttributeInfoBuilder(ATTR_SSO_IDENTIFIER);
        objClassBuilder.addAttributeInfo(ssoIdentifierDataBuilder.build());

        AttributeInfoBuilder countryBuilder = new AttributeInfoBuilder(ATTR_LOCATION_COUNTRY);
        countryBuilder.setReturnedByDefault(false);
        objClassBuilder.addAttributeInfo(countryBuilder.build());

        AttributeInfoBuilder countryCodeBuilder = new AttributeInfoBuilder(ATTR_LOCATION_COUNTRY_COODE);
        countryCodeBuilder.setReturnedByDefault(false);
        objClassBuilder.addAttributeInfo(countryCodeBuilder.build());

        AttributeInfoBuilder regionCodeBuilder = new AttributeInfoBuilder(ATTR_LOCATION_REGION_CODE);
        regionCodeBuilder.setReturnedByDefault(false);
        objClassBuilder.addAttributeInfo(regionCodeBuilder.build());

        AttributeInfoBuilder regionBuilder = new AttributeInfoBuilder(ATTR_LOCATION_REGION);
        regionBuilder.setReturnedByDefault(false);
        objClassBuilder.addAttributeInfo(regionBuilder.build());

        AttributeInfoBuilder cityBuilder = new AttributeInfoBuilder(ATTR_LOCATION_CITY);
        cityBuilder.setReturnedByDefault(false);
        objClassBuilder.addAttributeInfo(cityBuilder.build());

        AttributeInfoBuilder addressBuilder = new AttributeInfoBuilder(ATTR_LOCATION_ADDRESS);
        addressBuilder.setReturnedByDefault(false);
        objClassBuilder.addAttributeInfo(addressBuilder.build());

        AttributeInfoBuilder postalCodeBuilder = new AttributeInfoBuilder(ATTR_LOCATION_POSTAL_CODE);
        postalCodeBuilder.setReturnedByDefault(false);
        objClassBuilder.addAttributeInfo(postalCodeBuilder.build());

        AttributeInfoBuilder longitudeBuilder = new AttributeInfoBuilder(ATTR_LOCATION_LONGITUDE);
        longitudeBuilder.setReturnedByDefault(false);
        objClassBuilder.addAttributeInfo(longitudeBuilder.build());

        AttributeInfoBuilder latitudeBuilder = new AttributeInfoBuilder(ATTR_LOCATION_LATITUDE);
        latitudeBuilder.setReturnedByDefault(false);
        objClassBuilder.addAttributeInfo(latitudeBuilder.build());

        objClassBuilder.addAttributeInfo(OperationalAttributeInfos.ENABLE);     // active

        return objClassBuilder.build();
    }


    @Override
    public Uid create(ObjectClass objectClass, Set<Attribute> attributes, OperationOptions operationOptions) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {    // __ACCOUNT__
            return createUser(attributes);
        } else {
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    protected JSONObject callRequest(HttpEntityEnclosingRequestBase request, Object json, String contentType, String uid, String name) throws IOException {
        // don't log request here - password field !!!
        LOG.ok("request URI: {0}", request.getURI() + ", method: " + request.getMethod());
        request.setHeader("Content-Type", contentType);
        HttpEntity entity = new ByteArrayEntity(json.toString().getBytes("UTF-8"));
        request.setEntity(entity);
        CloseableHttpResponse response = execute(request);
        LOG.ok("response: {0}", response);
        JSONObject conflict = processSmartRecruiterResponseErrors(response, uid, name);
        // conflict detected, return fake response
        if (conflict != null) {
            return conflict;
        }

        if (response.getEntity() == null) {
            LOG.ok("response body is empty: {0}", response);
            return null;
        }
        String result = EntityUtils.toString(response.getEntity());
        LOG.ok("response body: {0}", result);
        closeResponse(response);
        return new JSONObject(result);
    }

    protected JSONObject callRequest(HttpRequestBase request, boolean parseResult) throws IOException {
        LOG.ok("request URI: {0}", request.getURI());
        request.setHeader("Content-Type", CONTENT_TYPE_JSON);
        CloseableHttpResponse response = null;
        response = execute(request);
        LOG.ok("response: {0}", response);
        processSmartRecruiterResponseErrors(response, null, null);

        if (!parseResult) {
            closeResponse(response);
            return null;
        }
        String result = EntityUtils.toString(response.getEntity());
        LOG.ok("response body: {0}", result);
        closeResponse(response);
        return new JSONObject(result);
    }

    protected JSONObject callRequest(HttpRequestBase request) throws IOException {
        LOG.ok("request URI: {0}", request.getURI());
        request.setHeader("Content-Type", CONTENT_TYPE_JSON);
        CloseableHttpResponse response = execute(request);
        LOG.ok("response: {0}", response);
        processSmartRecruiterResponseErrors(response, null, null);

        if (response.getEntity() == null) {
            LOG.ok("response body is empty: {0}", response);
            return null;
        }
        String result = EntityUtils.toString(response.getEntity());
        LOG.ok("response body: {0}", result);
        closeResponse(response);
        return new JSONObject(result);
    }

    private Uid createUser(Set<Attribute> attributes) {
        LOG.ok("createUser, attributes: {1}", attributes);

        // email
        String name = getStringAttr(attributes, Name.NAME);
        try {
            HttpGet requestSearch = new HttpGet(getConfiguration().getServiceAddress() + "?q=" + URLEncoder.encode(name, "UTF-8"));
            JSONObject result = callRequest(requestSearch);
            if (result.getInt("totalFound") > 0) {
                throw new AlreadyExistsException("user with same e-mail address already exists: " + result.getJSONArray("content"));
            }
        } catch (IOException ioe) {
            LOG.warn(ioe, "something wrong whe try to find: {0}", ioe);
        }

        JSONObject jo = new JSONObject();

        if (StringUtil.isBlank(name)) {
            throw new InvalidAttributeValueException("Missing mandatory attribute " + Name.NAME + " (email)");
        }
        if (name != null) {
            jo.put(ATTR_EMAIL, name);
        }


        String firstName = getStringAttr(attributes, ATTR_FIRST_NAME);
        if (StringUtil.isBlank(firstName)) {
            throw new InvalidAttributeValueException("Missing mandatory attribute " + ATTR_FIRST_NAME);
        }
        if (firstName != null) {
            jo.put(ATTR_FIRST_NAME, firstName);
        }

        String lastName = getStringAttr(attributes, ATTR_LAST_NAME);
        if (StringUtil.isBlank(lastName)) {
            throw new InvalidAttributeValueException("Missing mandatory attribute " + ATTR_LAST_NAME);
        }
        if (lastName != null) {
            jo.put(ATTR_LAST_NAME, lastName);
        }

        String role = getStringAttr(attributes, ATTR_ROLE);
        String roles = getStrongestRole(getMultiValAttr(attributes, ATTR_ROLES, null));
        if (roles != null) {
            role = roles; // get value from multivalue
        }
        if (StringUtil.isBlank(role)) {
            throw new InvalidAttributeValueException("Missing mandatory attribute " + ATTR_ROLE);
        }
        if (role != null) {
            jo.put(ATTR_ROLE, role);
        }

        final List<String> passwordList = new ArrayList<String>(1);
        GuardedString guardedPassword = getAttr(attributes, OperationalAttributeInfos.PASSWORD.getName(), GuardedString.class);
        if (guardedPassword != null) {
            guardedPassword.access(new GuardedString.Accessor() {
                @Override
                public void access(char[] chars) {
                    passwordList.add(new String(chars));
                }
            });
        }
        String password = null;
        if (!passwordList.isEmpty()) {
            password = passwordList.get(0);
        }

        Boolean enable = getAttr(attributes, OperationalAttributes.ENABLE_NAME, Boolean.class);

        putFieldIfExists(attributes, ATTR_EXTERNAL_DATA, jo);
        putFieldIfExists(attributes, ATTR_UPDATED_ON, jo); // read only?
        putFieldIfExists(attributes, ATTR_SSO_IDENTIFIER, jo);

        handleLocation(attributes, jo);

        LOG.ok("user request (without password): {0}", jo.toString());

        if (password != null) {
            jo.put(ATTR_PASSWORD, password);
        }

        try {
            HttpEntityEnclosingRequestBase request;
            request = new HttpPost(getConfiguration().getServiceAddress());
            JSONObject jores = callRequest(request, jo, CONTENT_TYPE_JSON, null, name);

            String newUid = jores.getString(ATTR_ID);
            LOG.info("response ID: {0}", newUid);

            if (!newUid.contains(CONFLICT)) {
                LOG.info("activating account");
                request = new HttpPut(getConfiguration().getServiceAddress() + "/" + newUid + "/activation");
                callRequest(request);

                if (enable == null || !enable) {
                    LOG.info("Deactivating account, because administrative status is set to: " + enable);
                    HttpDelete deactivatingRequest = new HttpDelete(getConfiguration().getServiceAddress() + "/" + newUid + "/activation");
                    callRequest(deactivatingRequest);
                }
            }
            return new Uid(newUid);
        } catch (IOException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private String getStrongestRole(String[] roles) {
        String ret = null;
        // from employee to more stronger roles - last is the best
        for (String available : AVAILABLE_ROLES) {
            if (roles != null) {
                for (String role : roles) {
                    if (available.equalsIgnoreCase(role)) {
                        ret = available;
                    }
                }
            }
        }

        return ret;
    }

    private Uid updateUser(Uid uid, Set<Attribute> attributes) {
        LOG.ok("updateUser, Uid: {0}, attributes: {1}", uid, attributes);
        if (attributes == null || attributes.isEmpty()) {
            LOG.ok("request ignored, empty attributes");
            return uid;
        }
        JSONArray jo = new JSONArray();
        String name = getStringAttr(attributes, Name.NAME);

        handlePatch(attributes, ATTR_FIRST_NAME, jo);
        handlePatch(attributes, ATTR_LAST_NAME, jo);
        handlePatch(attributes, Name.NAME, jo, ATTR_EMAIL);
        handlePatch(attributes, ATTR_ROLE, jo);
        handlePatch(attributes, ATTR_EXTERNAL_DATA, jo);
        handlePatch(attributes, ATTR_UPDATED_ON, jo);
        handlePatch(attributes, ATTR_SSO_IDENTIFIER, jo);

        handlePatch(attributes, ATTR_LOCATION_COUNTRY, jo, ATTR_LOCATION + "/" + ATTR_LOCATION_COUNTRY);
        handlePatch(attributes, ATTR_LOCATION_COUNTRY_COODE, jo, ATTR_LOCATION + "/" + ATTR_LOCATION_COUNTRY_COODE);
        handlePatch(attributes, ATTR_LOCATION_REGION_CODE, jo, ATTR_LOCATION + "/" + ATTR_LOCATION_REGION_CODE);
        handlePatch(attributes, ATTR_LOCATION_REGION, jo, ATTR_LOCATION + "/" + ATTR_LOCATION_REGION);
        handlePatch(attributes, ATTR_LOCATION_CITY, jo, ATTR_LOCATION + "/" + ATTR_LOCATION_CITY);
        handlePatch(attributes, ATTR_LOCATION_ADDRESS, jo, ATTR_LOCATION + "/" + ATTR_LOCATION_ADDRESS);
        handlePatch(attributes, ATTR_LOCATION_POSTAL_CODE, jo, ATTR_LOCATION + "/" + ATTR_LOCATION_POSTAL_CODE);
        handlePatch(attributes, ATTR_LOCATION_LONGITUDE, jo, ATTR_LOCATION + "/" + ATTR_LOCATION_LONGITUDE);
        handlePatch(attributes, ATTR_LOCATION_LATITUDE, jo, ATTR_LOCATION + "/" + ATTR_LOCATION_LATITUDE);

        handlePatch(attributes, OperationalAttributeInfos.PASSWORD.getName(), jo, ATTR_PASSWORD);

        Boolean enable = getAttr(attributes, OperationalAttributes.ENABLE_NAME, Boolean.class);

        LOG.ok("user request (without password): {0}", jo.toString());

        try {
            String newUid = uid.getUidValue();
            if (jo.length() > 0) {
                HttpEntityEnclosingRequestBase request;
                // update
                request = new HttpPatch(getConfiguration().getServiceAddress() + "/" + uid.getUidValue());
                JSONObject jores = callRequest(request, jo, CONTENT_TYPE_JSON_PATCH, newUid, name);
                newUid = jores.getString(ATTR_ID);
            } else {
                LOG.info("nothing changed, ignoring...");
            }

            if (!newUid.contains(CONFLICT)) {
                if (enable != null && enable) {
                    // enable
                    LOG.info("activating account");
                    HttpEntityEnclosingRequestBase activateRequest = new HttpPut(getConfiguration().getServiceAddress() + "/" + newUid + "/activation");
                    callRequest(activateRequest);
                } else if (enable != null && !enable) {
                    // disable
                    LOG.info("Deactivating account");
                    HttpDelete deactivatingRequest = new HttpDelete(getConfiguration().getServiceAddress() + "/" + newUid + "/activation");
                    callRequest(deactivatingRequest);
                }
            }

            return new Uid(newUid);
        } catch (IOException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private void handlePatch(Set<Attribute> attributes, String attrName, JSONArray jo) {
        handlePatch(attributes, attrName, jo, attrName);
    }

    private void handlePatch(Set<Attribute> attributes, String attrName, JSONArray jo, String path) {
        String value = null;
        // handle password
        if (OperationalAttributeInfos.PASSWORD.getName().equals(attrName)) {
            final List<String> passwordList = new ArrayList<String>(1);
            GuardedString guardedPassword = getAttr(attributes, OperationalAttributeInfos.PASSWORD.getName(), GuardedString.class);
            if (guardedPassword != null) {
                guardedPassword.access(new GuardedString.Accessor() {
                    @Override
                    public void access(char[] chars) {
                        passwordList.add(new String(chars));
                    }
                });
            }
            String password = null;
            if (!passwordList.isEmpty()) {
                value = passwordList.get(0);
            }
        } else {
            value = getStringAttr(attributes, attrName);
        }

        // handle also roles
        if (ATTR_ROLE.equals(attrName)) {
            String roles = getStrongestRole(getMultiValAttr(attributes, ATTR_ROLES, null));
            if (roles != null) {
                value = roles;
            }
        }

        if (value != null) {
            // add or update value
            JSONObject op = new JSONObject();
            op.put("op", "add");
            op.put("path", "/" + path);
            op.put("value", value);
            jo.put(op);
        } else if (attributes.contains(attrName) && value == null) {
            // remove value
            JSONObject op = new JSONObject();
            op.put("op", "remove");
            op.put("path", "/" + path);
            jo.put(op);
        }
    }

    private void handleLocation(Set<Attribute> attributes, JSONObject jo) {
        JSONObject joLocation = new JSONObject();

        putFieldIfExists(attributes, ATTR_LOCATION_COUNTRY, joLocation);
        putFieldIfExists(attributes, ATTR_LOCATION_COUNTRY_COODE, joLocation);
        putFieldIfExists(attributes, ATTR_LOCATION_REGION_CODE, joLocation);
        putFieldIfExists(attributes, ATTR_LOCATION_REGION, joLocation);
        putFieldIfExists(attributes, ATTR_LOCATION_CITY, joLocation);
        putFieldIfExists(attributes, ATTR_LOCATION_ADDRESS, joLocation);
        putFieldIfExists(attributes, ATTR_LOCATION_POSTAL_CODE, joLocation);
        putFieldIfExists(attributes, ATTR_LOCATION_LONGITUDE, joLocation);
        putFieldIfExists(attributes, ATTR_LOCATION_LATITUDE, joLocation);

        if (joLocation.length() > 0) {
            jo.append(ATTR_LOCATION, joLocation);
        }
    }

    private void putFieldIfExists(Set<Attribute> attributes, String fieldName, JSONObject jo) {
        String fieldValue = getStringAttr(attributes, fieldName);
        if (fieldValue != null) {
            jo.put(fieldName, fieldValue);
        }
    }


    @Override
    public void checkAlive() {
        test();
    }

    @Override
    public void delete(ObjectClass objectClass, Uid uid, OperationOptions operationOptions) {
        try {
            if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
                LOG.ok("disable user instead of delete, Uid: {0}", uid);
                HttpDelete request = new HttpDelete(getConfiguration().getServiceAddress() + "/" + uid.getUidValue() + "/activation");
                callRequest(request, false);
            } else {
                throw new UnsupportedOperationException("Unsupported object class " + objectClass);
            }
        } catch (IOException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    @Override
    public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> attributes, OperationOptions operationOptions) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            return updateUser(uid, attributes);
        } else {
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }


    @Override
    public FilterTranslator<SmartRecruitersFilter> createFilterTranslator(ObjectClass objectClass, OperationOptions operationOptions) {
        return new SmartRecruitersFilterTranslator();
    }

    public void executeQuery(ObjectClass objectClass, SmartRecruitersFilter query, ResultsHandler handler, OperationOptions options) {
        try {
            LOG.info("executeQuery on {0}, query: {1}, options: {2}", objectClass, query, options);
            if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
                //find by Uid (user Primary Key)
                if (query != null && query.byUid != null) {
                    HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + "/" + query.byUid);
                    JSONObject user = callRequest(request, true);
                    ConnectorObject connectorObject = convertUserToConnectorObject(user);
                    handler.handle(connectorObject);
                    //find by emailAddress
                } else if (query != null && query.byEmailAddress != null) {
                    HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + "?q=" + URLEncoder.encode(query.byEmailAddress, "UTF-8"));
                    handleUsers(request, handler, options, query);

                } else {
                    // find required page
                    String pageing = processPageOptions(options);
                    if (!StringUtil.isEmpty(pageing)) {
                        HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + "?" + pageing);
                        handleUsers(request, handler, options, null);
                    }
                    // find all
                    else {
                        int pageSize = getConfiguration().getPageSize();
                        int page = 0;
                        while (true) {
                            pageing = processPaging(page, pageSize);
                            HttpGet request = new HttpGet(getConfiguration().getServiceAddress() + "?" + pageing);
                            boolean finish = handleUsers(request, handler, options, null);
                            if (finish) {
                                break;
                            }
                            page++;
                        }
                    }
                }

            } else {
                // not found
                throw new UnsupportedOperationException("Unsupported object class " + objectClass);
            }
        } catch (IOException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private boolean handleUsers(HttpGet request, ResultsHandler handler, OperationOptions options, SmartRecruitersFilter query) throws IOException {
        JSONObject result = callRequest(request);
        LOG.ok("Number of users: {0}, offset: {1}, limit: {2} ", result.getInt("totalFound"), result.getInt("offset"), result.getInt("limit"));

        JSONArray users = result.getJSONArray("content");

        for (int i = 0; i < users.length(); i++) {
            if (i % 10 == 0) {
                LOG.ok("executeQuery: processing {0}. of {1} users", i, users.length());
            }
            // only basic fields
            JSONObject user = users.getJSONObject(i);
            if (query != null && query.byEmailAddress != null && !user.getString(ATTR_EMAIL).equalsIgnoreCase(query.byEmailAddress)) {
                LOG.info("Searching by e-mail {0} but not exact match found, ignoring user: {1}", query.byEmailAddress, user);
                continue;
            }
            if (locationToGet(options)) {
                HttpGet requestUserDetail = new HttpGet(getConfiguration().getServiceAddress() + "/" + user.getString(ATTR_ID));
                user = callRequest(requestUserDetail, true);
            }

            ConnectorObject connectorObject = convertUserToConnectorObject(user);
            boolean finish = !handler.handle(connectorObject);
            if (finish) {
                return true;
            }
        }

        // last page exceed
        if (getConfiguration().getPageSize() > users.length()) {
            return true;
        }
        // need next page
        return false;
    }

    private ConnectorObject convertUserToConnectorObject(JSONObject user) throws IOException {
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
        builder.setUid(new Uid(user.getString(ATTR_ID)));
        if (user.has(ATTR_EMAIL)) {
            builder.setName(user.getString(ATTR_EMAIL));
        }
        getIfExists(user, ATTR_FIRST_NAME, builder);
        getIfExists(user, ATTR_LAST_NAME, builder);
        getIfExists(user, ATTR_EXTERNAL_DATA, builder);
        getIfExists(user, ATTR_UPDATED_ON, builder);
        getIfExists(user, ATTR_SSO_IDENTIFIER, builder);
        getIfExists(user, ATTR_ROLE, builder);

        // role also as roles (multivalue)
        if (user.has(ATTR_ROLE)) {
            if (user.get(ATTR_ROLE) != null && !JSONObject.NULL.equals(user.get(ATTR_ROLE))) {
                addAttr(builder, ATTR_ROLES, user.getString(ATTR_ROLE));
            }
        }

        // location part
        if (user.has(ATTR_LOCATION)) {
            JSONObject location = user.getJSONObject(ATTR_LOCATION);
            getIfExists(location, ATTR_LOCATION_COUNTRY, builder);
            getIfExists(location, ATTR_LOCATION_COUNTRY_COODE, builder);
            getIfExists(location, ATTR_LOCATION_REGION_CODE, builder);
            getIfExists(location, ATTR_LOCATION_REGION, builder);
            getIfExists(location, ATTR_LOCATION_CITY, builder);
            getIfExists(location, ATTR_LOCATION_ADDRESS, builder);
            getIfExists(location, ATTR_LOCATION_POSTAL_CODE, builder);
            getIfExists(location, ATTR_LOCATION_LONGITUDE, builder);
            getIfExists(location, ATTR_LOCATION_LATITUDE, builder);
        }

        if (user.has(ATTR_ACTIVE)) {
            boolean enable = user.getBoolean(ATTR_ACTIVE);
            addAttr(builder, OperationalAttributes.ENABLE_NAME, enable);
        }

        ConnectorObject connectorObject = builder.build();
        LOG.ok("convertUserToConnectorObject, user: {0}, \n\tconnectorObject: {1}",
                user.getString(ATTR_ID), connectorObject);
        return connectorObject;
    }

    private String processPageOptions(OperationOptions options) {
        if (options != null) {
            Integer pageSize = options.getPageSize();
            Integer pagedResultsOffset = options.getPagedResultsOffset();
            if (pageSize != null && pagedResultsOffset != null) {

                return processPaging(pagedResultsOffset, pageSize);
            }
        }
        return "";
    }

    public String processPaging(int page, int pageSize) {
        StringBuilder queryBuilder = new StringBuilder();

        queryBuilder.append("&offset=").append(page * pageSize).append("&").append("limit=")
                .append(pageSize);

        return queryBuilder.toString();
    }


    private JSONObject processSmartRecruiterResponseErrors(CloseableHttpResponse response, String uid, String name) throws IOException {
        int statusCode = response.getStatusLine().getStatusCode();

        if (statusCode == 409) { //Conflict
            String result = null;
            try {
                result = EntityUtils.toString(response.getEntity());
            } catch (IOException e) {
                closeResponse(response);
                throw new ConnectorIOException("Error when trying to get response entity: " + response, e);
            }

            // conflict detected, trying to find user, If I don't see it, this is a conflict withim user from another domain and handle it specially
            try {
                HttpGet searchRequest = new HttpGet(getConfiguration().getServiceAddress() + "?q=" + URLEncoder.encode(name, "UTF-8"));
                JSONObject searchResponse = callRequest(searchRequest, true);
                // not visible
                if (searchResponse.getInt("totalFound") == 0) {
                    String conflictId = CONFLICT + CONFLICT_SEPARATOR + name + CONFLICT_SEPARATOR + (uid == null ? "" : uid);
                    JSONObject conflict = new JSONObject();
                    conflict.put(ATTR_ID, conflictId);

                    closeResponse(response);

                    return conflict;
                } else {
                    // visible - standard AlreadyExistsException
                }
            } catch (IOException e) {
                closeResponse(response);
                throw new ConnectorIOException("Error when trying to find user over e-mail: " + name, e);
            }


            closeResponse(response);
            throw new AlreadyExistsException("User already exists: " + result);
        }

        super.processResponseErrors(response);

        return null;
    }

    private void getIfExists(JSONObject object, String attrName, ConnectorObjectBuilder builder) {
        if (object.has(attrName)) {
            if (object.get(attrName) != null && !JSONObject.NULL.equals(object.get(attrName))) {
                addAttr(builder, attrName, object.getString(attrName));
            }
        }
    }


    private boolean locationToGet(OperationOptions options) {
        if (!getConfiguration().getReadLocation()) {
            return false;
        }

        if (options == null) {
            // not configured, get all data
            return true;
        }
        String[] attrsToGet = options.getAttributesToGet();
        if (attrsToGet == null) {
            // not configured, get all data
            return true;
        }
        for (String attrToGet : attrsToGet) {
            if (ATTR_LOCATION_COUNTRY.equals(attrToGet)) {
                return true;
            }
            if (ATTR_LOCATION_COUNTRY_COODE.equals(attrToGet)) {
                return true;
            }
            if (ATTR_LOCATION_REGION_CODE.equals(attrToGet)) {
                return true;
            }
            if (ATTR_LOCATION_REGION.equals(attrToGet)) {
                return true;
            }
            if (ATTR_LOCATION_CITY.equals(attrToGet)) {
                return true;
            }
            if (ATTR_LOCATION_ADDRESS.equals(attrToGet)) {
                return true;
            }
            if (ATTR_LOCATION_POSTAL_CODE.equals(attrToGet)) {
                return true;
            }
            if (ATTR_LOCATION_LONGITUDE.equals(attrToGet)) {
                return true;
            }
            if (ATTR_LOCATION_LATITUDE.equals(attrToGet)) {
                return true;
            }
        }

        return false;
    }
}
