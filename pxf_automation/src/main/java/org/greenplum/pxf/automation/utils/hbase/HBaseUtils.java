package org.greenplum.pxf.automation.utils.hbase;

import java.util.List;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

/**
 * Utilities for HBase
 */
public class HBaseUtils {

	/**
	 * get StringBuilder with print according to given {@link FilterList}
	 * 
	 * @param result as {@link StringBuilder}
	 * @param filterList {@link FilterList} with HBase filters
	 */
	public static void getFilterListPrint(StringBuilder result, FilterList filterList) {

		if (filterList == null || filterList.getFilters().size() == 0) {
			return;
		}

		if (result == null) {
			result = new StringBuilder();
		}

		List<Filter> list = filterList.getFilters();

		Operator operator = filterList.getOperator();
		String operatorString = (operator.equals(Operator.MUST_PASS_ALL)) ? " AND " : " OR ";

		// open current filter list
		result.append("[");

		int listIndex = 0;

		// go over each filter in the list
		for (Filter filter : list) {

			// if current filter is a FilterList, call the method again with it
			if (filter instanceof FilterList) {
				getFilterListPrint(result, (FilterList) filter);
			} else {

				// append filter to result
				result.append("(" + filter + ")");

				// if end of list and not FilterList, close the current list
				if (listIndex == list.size() - 1) {
					result.append("]");
				}
			}

			// if still not the end of list put the operator
			if (listIndex != list.size() - 1) {
				result.append(operatorString);
			}

			listIndex++;
		}
	}
}
