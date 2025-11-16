import { parse, generate } from '@databases/sqlite-ast';

// Parse a simple SELECT statement to see the structure
const sql = 'SELECT * FROM users WHERE id = 1';
const parsed = parse(sql).statements[0];

console.log('Parsed SELECT AST:');
console.log(JSON.stringify(parsed, null, 2));

// Generate it back
const generated = generate(parsed);
console.log('\nGenerated SQL:', generated);

// Try with table name
const parsed2 = parse('DELETE FROM users WHERE id = 1').statements[0];
console.log('\nParsed DELETE AST:');
console.log(JSON.stringify(parsed2, null, 2));
