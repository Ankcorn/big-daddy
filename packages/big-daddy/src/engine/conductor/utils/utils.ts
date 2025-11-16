/**
 * Utility functions for cache invalidation and query processing
 */

/**
 * Extract a key value for an indexed column(s) from row data in INSERT statement
 * Handles both Literal and Placeholder values from the AST
 * Returns null if any indexed column is NULL (NULL values are not indexed)
 */
export function extractKeyValueFromRow(
	columns: any[],
	row: any[],
	indexColumns: string[],
	params: any[],
): string | null {
	// Build a map of column names to values
	const rowData: Record<string, any> = {};

	columns.forEach((colIdent: any, colIndex: number) => {
		if (colIndex < row.length) {
			const value = row[colIndex];
			// Extract column name from Identifier
			const colName = typeof colIdent === 'string' ? colIdent : (colIdent as any).name;

			// Value is either a Literal or Placeholder
			if (typeof value === 'object' && value !== null) {
				if ('type' in value && value.type === 'Placeholder') {
					// It's a placeholder - get value from params
					const paramIndex = (value as any).parameterIndex;
					rowData[colName] = params[paramIndex] ?? null;
				} else if ('type' in value && value.type === 'Literal') {
					// It's a literal value
					rowData[colName] = (value as any).value;
				}
			} else {
				// Direct value (shouldn't happen with parsed AST, but handle it)
				rowData[colName] = value;
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
