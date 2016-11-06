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

import com.evolveum.polygon.rest.AbstractRestConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;

/**
 * @author gpalos
 *
 */
public class SmartRecruitersConfiguration extends AbstractRestConfiguration {

    private Integer pageSize = 100;

    private Boolean readLocation = true;

    @Override
    public String toString() {
        return "SmartRecruitersConfiguration{" +
                "username=" + getUsername() +
                ", serviceAddress=" + getServiceAddress() +
                ", authMethod=" + getAuthMethod() +
                ", tokenName=" + getTokenName() +
                ", pageSize=" + pageSize +
                ", readLocation=" + readLocation +
                '}';
    }

    @ConfigurationProperty(displayMessageKey = "smartrecruiters.config.pageSize",
            helpMessageKey = "smartrecruiters.config.pageSize.help")
    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }


    @ConfigurationProperty(displayMessageKey = "smartrecruiters.config.readLocation",
            helpMessageKey = "smartrecruiters.config.readLocation.help")
    public Boolean getReadLocation() {
        return readLocation;
    }

    public void setReadLocation(Boolean readLocation) {
        this.readLocation = readLocation;
    }
}
