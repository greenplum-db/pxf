package org.greenplum.pxf.automation.structures.profiles;

/** Represents PXF Profile with all it's components. */
public class Profile {

	private String name = "";
	private String description = "";
	private String fragmenter = "";
	private String accessor = "";
	private String resolver = "";

	public Profile(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFragmenter() {
		return fragmenter;
	}

	public void setFragmenter(String fragmenter) {
		this.fragmenter = fragmenter;
	}

	public String getAccessor() {
		return accessor;
	}

	public void setAccessor(String accessor) {
		this.accessor = accessor;
	}

	public String getResolver() {
		return resolver;
	}

	public void setResolver(String resolver) {
		this.resolver = resolver;
	}

	@Override
	public String toString() {

		return "Profile: " + name + " (fragmenter=" + fragmenter + ", accessor=" + accessor + ", resolver=" + resolver + ")";
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public boolean equals(Object obj) {

		Profile p = (Profile) obj;

		if (!p.getName().equals(getName())) {
			return false;
		}

		if (!p.getAccessor().equals(getAccessor())) {
			return false;
		}

		if (!p.getDescription().equals(getDescription())) {
			return false;
		}

		if (!p.getFragmenter().equals(getFragmenter())) {
			return false;
		}

		if (!p.getResolver().equals(getResolver())) {
			return false;
		}

		return true;
	}
}