import type {
	BinaryExpression,
	ColumnDefinition,
	CreateTableStatement,
	DeleteStatement,
	Expression,
	Identifier,
	InsertStatement,
	Literal,
	Placeholder,
	SelectStatement,
	Statement,
	TableConstraint,
	UpdateStatement,
} from "@databases/sqlite-ast";
import { generate } from "@databases/sqlite-ast";
import { logger } from "../../../logger";
import type { QueryResult, SqlParam } from "../types";

export function injectVirtualShard(
	statement:
		| UpdateStatement
		| InsertStatement
		| DeleteStatement
		| SelectStatement,
	params: SqlParam[],
	shardId: number,
): { modifiedStatement: Statement; modifiedParams: SqlParam[] } {
	try {
		if (statement.type === "InsertStatement") {
			// For INSERT, interleave shardId for each row
			// SQL will be: VALUES (col1, col2, col3, _virtualShard), (col1, col2, col3, _virtualShard), ...
			// So params must be: [val1, val2, val3, shardId, val4, val5, val6, shardId, ...]
			const insertStmt = statement as InsertStatement;
			const rows = insertStmt.values || [];

			// Count actual placeholders per row (not just columns - some may be literals)
			const interleavedParams: SqlParam[] = [];
			let paramIndex = 0;

			for (const row of rows) {
				// Add params for placeholders in this row (in order)
				for (const expr of row) {
					if (
						expr &&
						typeof expr === "object" &&
						"type" in expr &&
						expr.type === "Placeholder"
					) {
						interleavedParams.push(params[paramIndex]!);
						paramIndex++;
					}
				}
				// Add shard ID for this row
				interleavedParams.push(shardId);
			}

			return {
				modifiedStatement: insertColumn(insertStmt, params, shardId),
				modifiedParams: interleavedParams,
			};
		}
		const modifiedParams = [...params, shardId];
		const modifiedStatement = injectWhereFilter(statement, params);

		return {
			modifiedStatement: modifiedStatement || statement,
			modifiedParams,
		};
	} catch (error) {
		logger.warn`Failed to inject _virtualShard filter, using original statement ${{ error: error instanceof Error ? error.message : String(error) }}`;
		return { modifiedStatement: statement, modifiedParams: params };
	}
}

/**
 *
 * Inject virtual shard column on insert
 */
function insertColumn(
	stmt: InsertStatement,
	params: SqlParam[],
	_shardId: number,
): InsertStatement {
	const virtualShardIdentifier: Identifier = {
		type: "Identifier",
		name: "_virtualShard",
	};

	// For multi-row inserts, each row needs its own _virtualShard parameter
	// parameterIndex tracks position in params array, incrementing for each row
	let paramIndex = params.length;

	return {
		...stmt,
		columns: stmt.columns
			? [...stmt.columns, virtualShardIdentifier]
			: [virtualShardIdentifier],
		values: stmt.values
			? stmt.values.map((values) => {
					const rowWithShard = [
						...values,
						{ type: "Placeholder", parameterIndex: paramIndex } as Placeholder,
					];
					paramIndex++;
					return rowWithShard;
				})
			: [
					[
						{
							type: "Placeholder",
							parameterIndex: params.length,
						} as Placeholder,
					],
				],
	};
}

/**
 * Inject _virtualShard filter into a SELECT statement using AST manipulation
 */
function injectWhereFilter<
	TStatement extends DeleteStatement | SelectStatement | UpdateStatement,
>(stmt: TStatement, params: SqlParam[]): TStatement {
	const virtualShardFilter: BinaryExpression = {
		type: "BinaryExpression",
		operator: "=",
		left: {
			type: "Identifier",
			name: "_virtualShard",
		},
		right: {
			type: "Placeholder",
			parameterIndex: params.length,
		},
	};

	if (stmt.where) {
		return {
			...stmt,
			where: {
				type: "BinaryExpression",
				operator: "AND",
				left: stmt.where,
				right: virtualShardFilter,
			},
		};
	}

	return {
		...stmt,
		where: virtualShardFilter,
	};
}

/**
 * Inject _virtualShard column and create composite primary key
 *
 * This method modifies the CREATE TABLE AST to:
 * 1. Add _virtualShard INTEGER NOT NULL column as the first column
 * 2. Convert single-column PRIMARY KEY to composite: (_virtualShard, original_pk)
 * 3. For table-level PRIMARY KEY, prepend _virtualShard to the column list
 *
 * This allows the same primary key value to exist on multiple shards within
 * the same physical storage node, which is critical for resharding operations.
 */
export function injectVirtualShardColumn(
	statement: CreateTableStatement,
): string {
	const modifiedStatement = JSON.parse(
		JSON.stringify(statement),
	) as CreateTableStatement;

	// Track primary key columns
	const primaryKeyColumns: string[] = [];

	// Check for column-level PRIMARY KEY constraint
	for (let i = 0; i < modifiedStatement.columns.length; i++) {
		const col = modifiedStatement.columns[i]!;
		const pkConstraintIndex = col.constraints?.findIndex(
			(c) => c.constraint === "PRIMARY KEY",
		);

		if (pkConstraintIndex !== undefined && pkConstraintIndex >= 0) {
			// Found column-level PRIMARY KEY
			primaryKeyColumns.push(col.name.name);

			// Remove the PRIMARY KEY constraint from this column
			col.constraints?.splice(pkConstraintIndex, 1);
			break;
		}
	}

	// Check for table-level PRIMARY KEY constraint
	if (modifiedStatement.constraints) {
		const pkConstraintIndex = modifiedStatement.constraints.findIndex(
			(c) => c.constraint === "PRIMARY KEY",
		);

		if (pkConstraintIndex >= 0) {
			const pkConstraint = modifiedStatement.constraints[pkConstraintIndex]!;

			// Extract column names from the table-level PRIMARY KEY
			if (pkConstraint.columns) {
				primaryKeyColumns.push(...pkConstraint.columns.map((col) => col.name));
			}

			// Remove the original PRIMARY KEY constraint (we'll add a new one)
			modifiedStatement.constraints.splice(pkConstraintIndex, 1);
		}
	}

	// Add _virtualShard column at the end
	const virtualShardColumn: ColumnDefinition = {
		type: "ColumnDefinition",
		name: {
			type: "Identifier",
			name: "_virtualShard",
		},
		dataType: "INTEGER",
		constraints: [
			{
				type: "ColumnConstraint",
				constraint: "NOT NULL",
			},
			{
				type: "ColumnConstraint",
				constraint: "DEFAULT",
				value: {
					type: "Literal",
					value: 0,
					raw: "0",
				},
			},
		],
	};

	// Insert _virtualShard as the last column
	modifiedStatement.columns.push(virtualShardColumn);

	// Create composite PRIMARY KEY constraint with _virtualShard prepended
	if (primaryKeyColumns.length > 0) {
		const compositePKConstraint: TableConstraint = {
			type: "TableConstraint",
			constraint: "PRIMARY KEY",
			columns: [
				{ type: "Identifier", name: "_virtualShard" },
				...primaryKeyColumns.map((name) => ({
					type: "Identifier" as const,
					name,
				})),
			],
		};

		// Add or initialize constraints array
		if (!modifiedStatement.constraints) {
			modifiedStatement.constraints = [];
		}
		modifiedStatement.constraints.push(compositePKConstraint);
	}

	// Generate SQL from modified AST
	return generate(modifiedStatement);
}

/**
 * Check if a statement is a SELECT statement
 */
function isSelectStatement(statement: Statement): statement is SelectStatement {
	return statement?.type === "SelectStatement";
}

/**
 * Check if an expression is an aggregation function
 */
function isAggregationFunction(expr: Expression | undefined): boolean {
	if (!expr || expr.type !== "FunctionCall") return false;
	const funcName = ((expr as { name?: string }).name || "").toUpperCase();
	return ["COUNT", "SUM", "AVG", "MIN", "MAX"].includes(funcName);
}

/**
 * Reconstruct argument list from FunctionCall AST
 */
function reconstructArguments(args: unknown[] | undefined): string {
	if (!args || !Array.isArray(args)) {
		return "";
	}
	if (args.length === 0) {
		return "";
	}

	return args
		.map((arg) => {
			if (!arg || typeof arg !== "object") {
				return String(arg);
			}
			const argObj = arg as { type?: string; name?: string; value?: unknown };
			if (argObj.type === "AllColumns") {
				return "*";
			} else if (argObj.type === "Identifier") {
				return String(argObj.name || "col");
			} else if (argObj.type === "Literal") {
				return String(argObj.value);
			}
			// For unknown types, try to extract name
			if (typeof argObj.name === "string") {
				return argObj.name;
			}
			return "arg";
		})
		.join(", ");
}

/**
 * Get column name or alias from a select clause
 */
function getColumnName(selectClause: {
	alias?: string | { name: string };
	expression?: { type?: string; name?: string; args?: unknown[] };
}): string {
	if (selectClause.alias) {
		// alias can be a string or an Identifier object with a name property
		return typeof selectClause.alias === "string"
			? selectClause.alias
			: selectClause.alias.name;
	}
	if (selectClause.expression?.type === "FunctionCall") {
		// For function calls like COUNT(*), COUNT(name), etc., reconstruct the full function call
		// so we get the actual column name that SQLite returns
		const funcName = selectClause.expression.name || "FUNC";
		const args = selectClause.expression.args || [];
		const argString = reconstructArguments(args);
		return `${funcName}(${argString})`;
	}
	if (selectClause.expression?.name) {
		return selectClause.expression.name;
	}
	return "result";
}

/**
 * Check if a SELECT statement has aggregation functions
 */
function hasAggregations(statement: SelectStatement): boolean {
	if (!statement.select || !Array.isArray(statement.select)) return false;
	return statement.select.some((col: { expression?: Expression }) =>
		isAggregationFunction(col.expression),
	);
}

/**
 * Merge aggregation results from multiple shards
 */
function mergeAggregations(
	results: QueryResult[],
	statement: SelectStatement,
): Record<string, unknown>[] {
	if (results.length === 0) return [];
	if (results.length === 1) return results[0]!.rows;

	// Collect all rows from shards
	const allRows = results.flatMap((r) => r.rows);
	if (allRows.length === 0) return [];

	// Get the actual column names from the first row
	const actualColumnNames = allRows[0] ? Object.keys(allRows[0]) : [];

	// Get select columns to identify aggregation types
	const selectCols = statement.select || [];

	// Build merged row with aggregation logic
	const mergedRow: Record<string, unknown> = {};

	for (let i = 0; i < selectCols.length; i++) {
		const col = selectCols[i]!;
		const colName = getColumnName(col);
		const expr = col.expression;

		// Try to find the actual column name in the returned rows
		// This handles cases where SQLite returns a different name than we reconstruct
		let actualColName = colName;
		if (!actualColumnNames.includes(colName)) {
			// Try lowercase version
			const lowerColName = colName.toLowerCase();
			if (actualColumnNames.includes(lowerColName)) {
				actualColName = lowerColName;
			} else {
				// Try to find a column that starts with the function name (case-insensitive)
				const funcName =
					expr?.type === "FunctionCall" ? (expr.name || "").toLowerCase() : "";
				const matchingCol = actualColumnNames.find((c) =>
					c.toLowerCase().startsWith(funcName),
				);
				if (matchingCol) {
					actualColName = matchingCol;
				}
			}
		}

		if (expr?.type === "FunctionCall") {
			const funcName = (expr.name || "").toUpperCase();

			if (funcName === "COUNT") {
				// SUM all count values
				const countValue = allRows.reduce((sum, row) => {
					const val = Number(row[actualColName]) || 0;
					return sum + val;
				}, 0);
				mergedRow[colName] = countValue;
			} else if (funcName === "SUM") {
				// SUM all sum values
				mergedRow[colName] = allRows.reduce(
					(sum, row) => sum + (Number(row[actualColName]) || 0),
					0,
				);
			} else if (funcName === "MIN") {
				// Take minimum of all values
				const values = allRows
					.map((row) => row[actualColName])
					.filter((v): v is number => v != null && typeof v === "number");
				mergedRow[colName] = values.length > 0 ? Math.min(...values) : null;
			} else if (funcName === "MAX") {
				// Take maximum of all values
				const values = allRows
					.map((row) => row[actualColName])
					.filter((v): v is number => v != null && typeof v === "number");
				mergedRow[colName] = values.length > 0 ? Math.max(...values) : null;
			} else if (funcName === "AVG") {
				// For AVG, we need to recalculate
				// Ideally we'd have SUM and COUNT separately, but as a fallback average the averages
				const values = allRows
					.map((row) => row[actualColName])
					.filter((v): v is number => v != null && typeof v === "number");
				if (values.length > 0) {
					mergedRow[colName] =
						values.reduce((a, b) => a + b, 0) / values.length;
				} else {
					mergedRow[colName] = null;
				}
			}
		} else {
			// Non-aggregated column - use first value (for GROUP BY scenarios)
			mergedRow[colName] = allRows[0]?.[actualColName];
		}
	}

	return [mergedRow];
}

/**
 * Merge results from multiple shards
 * Handles SELECT (with aggregations), INSERT, UPDATE, and DELETE statements
 */
export function mergeResultsSimple(
	results: QueryResult[],
	statement:
		| SelectStatement
		| InsertStatement
		| UpdateStatement
		| DeleteStatement,
): QueryResult {
	if (isSelectStatement(statement)) {
		// Check if this SELECT has aggregation functions
		if (hasAggregations(statement)) {
			const mergedRows = mergeAggregations(results, statement);
			return {
				rows: mergedRows,
				rowsAffected: mergedRows.length,
			};
		}

		// Regular SELECT - merge all rows from shards
		const mergedRows = results.flatMap((r) => r.rows);

		// Strip _virtualShard from result rows (hidden column should not be visible to user)
		const cleanedRows = mergedRows.map((row) => {
			const { _virtualShard: _, ...cleaned } = row as Record<string, unknown>;
			return cleaned;
		});

		return {
			rows: cleanedRows,
			rowsAffected: cleanedRows.length,
		};
	} else {
		// For INSERT/UPDATE/DELETE, sum the rowsAffected
		const totalAffected = results.reduce(
			(sum, r) => sum + (r.rowsAffected || 0),
			0,
		);
		return {
			rows: [],
			rowsAffected: totalAffected,
		};
	}
}

/**
 * Extract key value from a row for index invalidation
 */
export function extractKeyValueFromRow(
	columns: Array<{ name: string }>,
	row: Expression[],
	indexColumns: string[],
	params: SqlParam[],
): string | null {
	const values: SqlParam[] = [];

	for (const colName of indexColumns) {
		const columnIndex = columns.findIndex((col) => col.name === colName);
		if (columnIndex === -1) {
			return null; // Column not in INSERT
		}

		const valueExpression = row[columnIndex];
		const value = extractValueFromExpression(valueExpression, params);

		if (value === null || value === undefined) {
			return null; // NULL values are not indexed
		}

		values.push(value);
	}

	if (values.length !== indexColumns.length) {
		return null;
	}

	// Build the key value (same format as topology uses)
	return indexColumns.length === 1 ? String(values[0]) : JSON.stringify(values);
}

/**
 * Extract value from an expression node (for cache invalidation)
 */
function extractValueFromExpression(
	expression: Expression | undefined,
	params: SqlParam[],
): SqlParam {
	if (!expression) {
		return null;
	}
	if (expression.type === "Literal") {
		return (expression as Literal).value as SqlParam;
	} else if (expression.type === "Placeholder") {
		return params[(expression as Placeholder).parameterIndex]!;
	}
	return null;
}

/**
 * Hash a value to a shard ID
 * Uses the same algorithm as Topology.hashToShardId for consistency
 *
 * @param value - The value to hash (typically the shard key value)
 * @param numShards - Number of shards to distribute across
 * @returns Shard ID (0-based)
 */
export function hashToShardId(value: SqlParam, numShards: number): number {
	const strValue = String(value);
	let hash = 0;
	for (let i = 0; i < strValue.length; i++) {
		hash = (hash << 5) - hash + strValue.charCodeAt(i);
		hash = hash & hash;
	}
	return Math.abs(hash) % numShards;
}
