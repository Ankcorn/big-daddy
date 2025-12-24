/**
 * AST utility functions for working with parsed SQL statements
 */

import type {
	DeleteStatement,
	Expression,
	Literal,
	Placeholder,
	SelectStatement,
	Statement,
	UpdateStatement,
} from "@databases/sqlite-ast";

/** SQL parameter type for query bindings */
export type SqlParam = string | number | boolean | null;

/**
 * Query type enumeration for categorizing SQL statements
 * (Note: Not used by Storage layer, but needed for logging and query planning)
 */
export type QueryType =
	| "SELECT"
	| "INSERT"
	| "UPDATE"
	| "DELETE"
	| "CREATE"
	| "DROP"
	| "ALTER"
	| "PRAGMA"
	| "UNKNOWN";

/**
 * Extract the table name from a parsed SQL statement
 */
export function extractTableName(statement: Statement): string | null {
	if ("from" in statement && statement.from) {
		return statement.from.name;
	}
	if ("table" in statement && statement.table) {
		return statement.table.name;
	}
	return null;
}

/**
 * Extract the WHERE clause from a statement, if it has one
 */
export function extractWhereClause(statement: Statement): Expression | null {
	switch (statement.type) {
		case "SelectStatement":
			return (statement as SelectStatement).where || null;
		case "UpdateStatement":
			return (statement as UpdateStatement).where || null;
		case "DeleteStatement":
			return (statement as DeleteStatement).where || null;
		default:
			return null;
	}
}

/**
 * Extract a literal value from an AST expression
 * Handles both literal values and parameter placeholders
 */
export function extractValueFromExpression(
	expr: Expression,
	params: SqlParam[],
): SqlParam {
	if (expr.type === "Literal") {
		return (expr as Literal).value as SqlParam;
	} else if (expr.type === "Placeholder") {
		// Use the parameterIndex from the AST to get the correct parameter
		const paramIndex = (expr as Placeholder).parameterIndex;
		if (paramIndex !== undefined && paramIndex < params.length) {
			return params[paramIndex]!;
		}
	}
	return null;
}

/**
 * Determine the query type from a parsed SQL statement
 */
export function getQueryType(statement: Statement): QueryType {
	switch (statement.type) {
		case "SelectStatement":
			return "SELECT";
		case "InsertStatement":
			return "INSERT";
		case "UpdateStatement":
			return "UPDATE";
		case "DeleteStatement":
			return "DELETE";
		case "CreateTableStatement":
			return "CREATE";
		case "AlterTableStatement":
			return "ALTER";
		case "CreateIndexStatement":
			return "CREATE";
		case "DropTableStatement":
			return "DROP";
		default:
			return "UNKNOWN";
	}
}

/**
 * Build a parameterized query from template literal parts
 */
export function buildQuery(
	strings: TemplateStringsArray,
	values: SqlParam[],
): { query: string; params: SqlParam[] } {
	let query = "";
	const params: SqlParam[] = [];

	for (let i = 0; i < strings.length; i++) {
		query += strings[i];

		if (i < values.length) {
			// Add parameter placeholder
			query += "?";
			params.push(values[i]!);
		}
	}

	return { query, params };
}
