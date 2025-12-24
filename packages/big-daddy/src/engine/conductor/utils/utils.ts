/**
 * Utility functions for cache invalidation and query processing
 */

import type {
	Expression,
	Identifier,
	Literal,
	Placeholder,
} from "@databases/sqlite-ast";
import type { SqlParam } from "../types";

/**
 * Extract a key value for an indexed column(s) from row data in INSERT statement
 * Handles both Literal and Placeholder values from the AST
 * Returns null if any indexed column is NULL (NULL values are not indexed)
 */
export function extractKeyValueFromRow(
	columns: Identifier[],
	row: Expression[],
	indexColumns: string[],
	params: SqlParam[],
): string | null {
	// Build a map of column names to values
	const rowData: Record<string, SqlParam> = {};

	columns.forEach((colIdent: Identifier, colIndex: number) => {
		if (colIndex < row.length) {
			const value = row[colIndex]!;
			// Extract column name from Identifier
			const colName = colIdent.name;

			// Value is either a Literal or Placeholder
			if (typeof value === "object" && value !== null) {
				if ("type" in value && value.type === "Placeholder") {
					// It's a placeholder - get value from params
					const paramIndex = (value as Placeholder).parameterIndex;
					rowData[colName] = params[paramIndex] ?? null;
				} else if ("type" in value && value.type === "Literal") {
					// It's a literal value
					rowData[colName] = (value as Literal).value as SqlParam;
				}
			} else {
				// Direct value (shouldn't happen with parsed AST, but handle it)
				rowData[colName] = value as SqlParam;
			}
		}
	});

	// Extract key value from indexed columns
	if (indexColumns.length === 1) {
		const value = rowData[indexColumns[0]!];
		if (value === null || value === undefined) {
			return null;
		}
		return String(value);
	} else {
		// Composite index - build key from all column values
		const values = indexColumns.map((col) => rowData[col]);
		if (values.some((v) => v === null || v === undefined)) {
			return null;
		}
		return JSON.stringify(values);
	}
}
