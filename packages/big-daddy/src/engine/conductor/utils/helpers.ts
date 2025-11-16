import { parse, generate } from '@databases/sqlite-ast';
import type {
	SelectStatement,
	UpdateStatement,
	DeleteStatement,
	CreateTableStatement,
	BinaryExpression,
	ColumnDefinition,
	TableConstraint,
	Statement,
	InsertStatement,
	Identifier,
	Expression,
	Literal,
	Placeholder,
} from '@databases/sqlite-ast';
import type { QueryResult, ShardStats } from '../types';
import { logger } from '../../../logger';

export function injectVirtualShard(
	statement: UpdateStatement | InsertStatement | DeleteStatement | SelectStatement,
	params: any[],
	shardId: number,
): { modifiedStatement: Statement; modifiedParams: any[] } {
	try {
		if (statement.type === 'InsertStatement') {
			// For INSERT, append shardId to params and pass both the modified statement and params
			return { modifiedStatement: insertColumn(statement, params), modifiedParams: [...params, shardId] };
		}
		const modifiedParams = [...params, shardId];
		const modifiedStatement = injectWhereFilter(statement, params);

		return { modifiedStatement: modifiedStatement || statement, modifiedParams };
	} catch (error) {
		logger.warn('Failed to inject _virtualShard filter, using original statement', {
			error: error instanceof Error ? error.message : String(error),
		});
		return { modifiedStatement: statement, modifiedParams: params };
	}
}

/**
 *
 * Inject virtual shard column on insert
 */
function insertColumn(stmt: InsertStatement, params: any[]): InsertStatement {
	const virtualShardIdentifier: Identifier = {
		type: 'Identifier',
		name: '_virtualShard',
	};
	const virtualShardValue: Placeholder = { type: 'Placeholder', parameterIndex: params.length };

	return {
		...stmt,
		columns: stmt.columns ? [...stmt.columns, virtualShardIdentifier] : [virtualShardIdentifier],
		values: stmt.values ? stmt.values.map((values) => [...values, virtualShardValue]) : [[virtualShardValue]],
	};
}

/**
 * Inject _virtualShard filter into a SELECT statement using AST manipulation
 */
function injectWhereFilter<TStatement extends DeleteStatement | SelectStatement | UpdateStatement>(
	stmt: TStatement,
	params: any[],
): TStatement {
	const virtualShardFilter: BinaryExpression = {
		type: 'BinaryExpression',
		operator: '=',
		left: {
			type: 'Identifier',
			name: '_virtualShard',
		},
		right: {
			type: 'Placeholder',
			parameterIndex: params.length,
		},
	};

	if (stmt.where) {
		return {
			...stmt,
			where: {
				type: 'BinaryExpression',
				operator: 'AND',
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
export function injectVirtualShardColumn(statement: CreateTableStatement): string {
	const modifiedStatement = JSON.parse(JSON.stringify(statement)) as CreateTableStatement;

	// Track primary key columns
	const primaryKeyColumns: string[] = [];

	// Check for column-level PRIMARY KEY constraint
	for (let i = 0; i < modifiedStatement.columns.length; i++) {
		const col = modifiedStatement.columns[i];
		const pkConstraintIndex = col.constraints?.findIndex((c) => c.constraint === 'PRIMARY KEY');

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
		const pkConstraintIndex = modifiedStatement.constraints.findIndex((c) => c.constraint === 'PRIMARY KEY');

		if (pkConstraintIndex >= 0) {
			const pkConstraint = modifiedStatement.constraints[pkConstraintIndex];

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
		type: 'ColumnDefinition',
		name: {
			type: 'Identifier',
			name: '_virtualShard',
		},
		dataType: 'INTEGER',
		constraints: [
			{
				type: 'ColumnConstraint',
				constraint: 'NOT NULL',
			},
			{
				type: 'ColumnConstraint',
				constraint: 'DEFAULT',
				value: {
					type: 'Literal',
					value: 0,
					raw: '0',
				},
			},
		],
	};

	// Insert _virtualShard as the last column
	modifiedStatement.columns.push(virtualShardColumn);

	// Create composite PRIMARY KEY constraint with _virtualShard appended
	if (primaryKeyColumns.length > 0) {
		const compositePKConstraint: TableConstraint = {
			type: 'TableConstraint',
			constraint: 'PRIMARY KEY',
			columns: [...primaryKeyColumns.map((name) => ({ type: 'Identifier' as const, name })), { type: 'Identifier', name: '_virtualShard' }],
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
 * Merge results from multiple shards (simple version)
 */
export function mergeResultsSimple(results: QueryResult[], isSelect: boolean): QueryResult {
	if (isSelect) {
		// Merge rows from all shards
		const mergedRows = results.flatMap((r) => r.rows);

		// Strip _virtualShard from result rows (hidden column should not be visible to user)
		const cleanedRows = mergedRows.map((row) => {
			const cleaned = { ...row };
			delete (cleaned as any)._virtualShard;
			return cleaned;
		});

		return {
			rows: cleanedRows,
			rowsAffected: cleanedRows.length,
		};
	} else {
		// For INSERT/UPDATE/DELETE, sum the rowsAffected
		const totalAffected = results.reduce((sum, r) => sum + (r.rowsAffected || 0), 0);
		return {
			rows: [],
			rowsAffected: totalAffected,
		};
	}
}

/**
 * Extract key value from a row for index invalidation
 */
export function extractKeyValueFromRow(columns: Array<{ name: string }>, row: any[], indexColumns: string[], params: any[]): string | null {
	const values: any[] = [];

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
function extractValueFromExpression(expression: any, params: any[]): any {
	if (!expression) {
		return null;
	}
	if (expression.type === 'Literal') {
		return expression.value;
	} else if (expression.type === 'Placeholder') {
		return params[expression.parameterIndex];
	}
	return null;
}
