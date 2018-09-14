package org.greenplum.pxf.automation.utils.json;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import org.greenplum.pxf.automation.structures.data.MetaTableData;
import org.greenplum.pxf.automation.structures.data.MetaTableDataCollection;
import org.greenplum.pxf.automation.structures.tables.basic.Table;

/*
 * Utility Class that is used to read json format data and return
 * org.greenplum.pxf.automation.structures.tables.basic.Table. This class is not thread safe.
 */
public class JsonUtils {

	public static List<Table> getTablesDataFromFile(String filename)
			throws JsonParseException, JsonMappingException, IOException {

		// Initialize the collection of table/data meta info from JSON format file.
		MetaTableDataCollection mtdCollection = new ObjectMapper().readValue(new File(filename), MetaTableDataCollection.class);

		List<Table> lst = new ArrayList<Table>();
		for (MetaTableData mtd : mtdCollection.get("tables")) {
			lst.add(new Table(mtd.getTableName(), mtd.generateTableColumnsFields(), mtd.getDataPattern()));
		}

		return lst;
	}

	/**
	 * Converts json representation to object of given type.
	 *
	 * @param content json representation of object
	 * @param clazz type of target object
	 * @throws IOException if <b>content</b> is malformed
	 */
	public static <T> T deserialize(String content, Class<T> clazz) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.readValue(content, clazz);
	}
}
