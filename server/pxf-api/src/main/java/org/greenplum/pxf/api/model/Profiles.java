package org.greenplum.pxf.api.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement(name = "profiles")
@XmlAccessorType(XmlAccessType.FIELD)
public class Profiles {

    @XmlElement(name = "profile")
    private List<BaseProfile> profiles;

    public List<BaseProfile> getProfiles() {
        return profiles;
    }

    public void setProfiles(List<BaseProfile> profiles) {
        this.profiles = profiles;
    }
}
