import { describe, it, expect } from 'vitest';
import { parse, generate } from '@databases/sqlite-ast';

describe('AST Structure Understanding', () => {
	it('should understand SELECT * FROM table structure', () => {
		const sql = 'SELECT * FROM users WHERE id = 1';
		const parsed = parse(sql).statements[0];

		console.log('SELECT AST:', JSON.stringify(parsed, null, 2));

		const generated = generate(parsed);
		console.log('Generated SQL:', generated);

		expect(generated).toContain('SELECT');
		expect(generated).toContain('FROM');
		expect(generated).toContain('users');
	});

	it('should understand DELETE FROM table structure', () => {
		const sql = 'DELETE FROM users WHERE id = 1';
		const parsed = parse(sql).statements[0];

		console.log('DELETE AST:', JSON.stringify(parsed, null, 2));
		console.log('DELETE table property:', JSON.stringify((parsed as any).table, null, 2));

		// Check the structure
		expect((parsed as any).table).toBeDefined();
		expect((parsed as any).where).toBeDefined();
	});

	it('should manually construct SELECT statement - attempt 1 (Identifier)', () => {
		// Try with Identifier object
		const selectStatement: any = {
			type: 'SelectStatement',
			select: [
				{
					type: 'SelectColumn',
					expression: {
						type: 'Identifier',
						name: '*',
					},
				},
			],
			from: [
				{
					type: 'Table',
					name: {
						type: 'Identifier',
						name: 'users',
					},
				},
			],
			where: {
				type: 'BinaryExpression',
				operator: '=',
				left: {
					type: 'Identifier',
					name: 'id',
				},
				right: {
					type: 'Literal',
					value: 1,
					raw: '1',
				},
			},
		};

		const generated = generate(selectStatement);
		console.log('Manually constructed SELECT (Identifier):', generated);

		expect(generated).toContain('SELECT');
		expect(generated).toContain('*');
		expect(generated).toContain('FROM');
		// This will likely fail - showing table name is lost
	});

	it('should manually construct SELECT statement - attempt 2 (string)', () => {
		// Try with string table name
		const selectStatement: any = {
			type: 'SelectStatement',
			select: [
				{
					type: 'SelectColumn',
					expression: {
						type: 'Identifier',
						name: '*',
					},
				},
			],
			from: [
				{
					type: 'Table',
					name: 'users', // Try string directly
				},
			],
			where: {
				type: 'BinaryExpression',
				operator: '=',
				left: {
					type: 'Identifier',
					name: 'id',
				},
				right: {
					type: 'Literal',
					value: 1,
					raw: '1',
				},
			},
		};

		const generated = generate(selectStatement);
		console.log('Manually constructed SELECT (string):', generated);

		expect(generated).toContain('SELECT');
		expect(generated).toContain('*');
		expect(generated).toContain('FROM');
		expect(generated).toContain('users');
	});

	it('should manually construct SELECT using DELETE table directly', () => {
		// Parse a DELETE to get the table structure, then use it in SELECT
		const deleteSql = 'DELETE FROM users WHERE id = 1';
		const deleteStmt = parse(deleteSql).statements[0] as any;

		console.log('Using DELETE table in SELECT - table object:', JSON.stringify(deleteStmt.table, null, 2));

		const selectStatement: any = {
			type: 'SelectStatement',
			select: [
				{
					type: 'SelectColumn',
					expression: {
						type: 'Identifier',
						name: '*',
					},
				},
			],
			from: [deleteStmt.table], // Reuse the table from DELETE
			where: deleteStmt.where,
		};

		const generated = generate(selectStatement);
		console.log('Manually constructed SELECT (reusing DELETE table):', generated);

		expect(generated).toContain('SELECT');
		expect(generated).toContain('*');
		expect(generated).toContain('FROM');
		expect(generated).toContain('users');
	});
});
