/**
 * AST utility functions for working with parsed SQL statements
 */

import type {
	Statement,
	SelectStatement,
	InsertStatement,
	UpdateStatement,
	DeleteStatement,
} from '@databases/sqlite-ast';
import type { QueryType } from '../storage';

/**
 * Extract the table name from a parsed SQL statement
 */
export function extractTableName(statement: Statement): string | null {
	switch (statement.type) {
		case 'SelectStatement':
			return (statement as SelectStatement).from?.name || null;
		case 'InsertStatement':
			return (statement as InsertStatement).table.name;
		case 'UpdateStatement':
			return (statement as UpdateStatement).table.name;
		case 'DeleteStatement':
			return (statement as DeleteStatement).table.name;
		default:
			return null;
	}
}

/**
 * Extract the WHERE clause from a statement, if it has one
 */
export function extractWhereClause(statement: Statement): any | null {
	switch (statement.type) {
		case 'SelectStatement':
			return (statement as SelectStatement).where || null;
		case 'UpdateStatement':
			return (statement as UpdateStatement).where || null;
		case 'DeleteStatement':
			return (statement as DeleteStatement).where || null;
		default:
			return null;
	}
}

/**
 * Extract a literal value from an AST expression
 * Handles both literal values and parameter placeholders
 */
export function extractValueFromExpression(expr: any, params: any[]): any {
	if (expr.type === 'Literal') {
		return expr.value;
	} else if (expr.type === 'Placeholder') {
		// Use the parameterIndex from the AST to get the correct parameter
		const paramIndex = expr.parameterIndex;
		if (paramIndex !== undefined && paramIndex < params.length) {
			return params[paramIndex];
		}
	}
	return null;
}

/**
 * Determine the query type from a parsed SQL statement
 */
export function getQueryType(statement: Statement): QueryType {
	switch (statement.type) {
		case 'SelectStatement':
			return 'SELECT';
		case 'InsertStatement':
			return 'INSERT';
		case 'UpdateStatement':
			return 'UPDATE';
		case 'DeleteStatement':
			return 'DELETE';
		case 'CreateTableStatement':
			return 'CREATE';
		case 'AlterTableStatement':
			return 'ALTER';
		case 'CreateIndexStatement':
			return 'CREATE';
		default:
			return 'UNKNOWN';
	}
}

/**
 * Build a parameterized query from template literal parts
 */
export function buildQuery(strings: TemplateStringsArray, values: any[]): { query: string; params: any[] } {
	let query = '';
	const params: any[] = [];

	for (let i = 0; i < strings.length; i++) {
		query += strings[i];

		if (i < values.length) {
			// Add parameter placeholder
			query += '?';
			params.push(values[i]);
		}
	}

	return { query, params };
}
