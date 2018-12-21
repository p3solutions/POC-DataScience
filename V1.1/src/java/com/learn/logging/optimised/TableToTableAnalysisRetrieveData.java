package com.learn.logging.optimised;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.collections4.map.HashedMap;

import com.learn.logging.optimised.TableToTableDataAnalyzeDTO.ColumnDetails;
import com.learn.logging.optimised.TableToTableDataAnalyzeDTO.PrimaryTable;
import com.learn.logging.optimised.TableToTableDataAnalyzeDTO.SecondaryTable;

public class TableToTableAnalysisRetrieveData {
	static HashMap<String, List<String>> sec = new HashMap<>();
	static List<String> pCList;
	static String priTable;
	public static void main(String[] args) {
		TableToTableDataAnalyzeDTO obj = new TableToTableDataAnalyzeDTO();
		//fetching primary table details
		PrimaryTable priTabObject = obj.getPrimaryTable();
//		String priTable =  priTabObject.getTableName();
		List<ColumnDetails> primaryColumnList = priTabObject.getPrimaryColumnList();
//		pCList = new ArrayList();
		for(ColumnDetails i : primaryColumnList) {
			pCList.add(i.getColumnName());
		}
		
		//fetching secondary table(s) details
		List<SecondaryTable> secTabList = obj.getSecondaryTableList();
		List<ColumnDetails> sCList;
		for(SecondaryTable i: secTabList) {
			String secTabName = i.getTableName();
			List<ColumnDetails> secColList = i.getSecondaryColumnList();;
			sCList = i.getSecondaryColumnList();
			List<String> l = new ArrayList<>();
			for(ColumnDetails j : secColList) {
				l.add(j.getColumnName());
			}

			sec.put(secTabName, l);
		}
	}
}


//PrimaryTable k = new PrimaryTable();
//List<ColumnDetails> pCList = new List<ColumnDetails>();
//for(ColumnDetails i : pCList) {
//	i.setColumnName("actor_id");
//}
//k.setPrimaryColumnList(pCList);
//obj.setPrimaryTable(k);
