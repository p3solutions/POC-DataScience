package com.learn.logging.optimised;


import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TableToTableDataAnalyzeDTO {

	private String userId;
	private String workspaceId;
	private PrimaryTable primaryTable;
	private List<SecondaryTable> secondaryTableList;

	@Data
	@Builder
	@AllArgsConstructor
	@NoArgsConstructor
	public static class PrimaryTable {
		private String tableId;
		private String tableName;
		private List<ColumnDetails> primaryColumnList;

	}

	@Data
	@Builder
	@AllArgsConstructor
	@NoArgsConstructor
	public static class SecondaryTable {
		private String tableId;
		private String tableName;
		private List<ColumnDetails> secondaryColumnList;

	}

	@Data
	@Builder
	@AllArgsConstructor
	@NoArgsConstructor
	public static class ColumnDetails {
		private String columnId;
		private String columnName;
	}

}

