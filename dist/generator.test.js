import { describe, it, expect } from "vitest";
import { parse } from "./parser";
import { generate } from "./generator";
describe("generator", () => {
    const testCases = [
        // SELECT statements
        "SELECT name, email FROM users",
        "SELECT name, email FROM users WHERE id = 1",
        "SELECT id, name FROM products WHERE category = 'electronics'",
        "SELECT DISTINCT category FROM products",
        "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id",
        "SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id",
        "SELECT users.name, orders.total FROM users RIGHT JOIN orders ON users.id = orders.user_id",
        "SELECT users.name, orders.total FROM users INNER JOIN orders ON users.id = orders.user_id",
        "SELECT name, age FROM users ORDER BY age DESC",
        "SELECT name, age FROM users ORDER BY age ASC",
        "SELECT name FROM users LIMIT 10 OFFSET 20",
        "SELECT name FROM users LIMIT 10",
        "SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 5",
        "SELECT category, COUNT(*) FROM products GROUP BY category",
        "SELECT name AS user_name, email AS user_email FROM users",
        "SELECT * FROM users WHERE age > 18 AND status = 'active'",
        "SELECT * FROM users WHERE age > 18 OR status = 'active'",
        "SELECT name FROM users WHERE id IN (1, 2, 3)",
        "SELECT name FROM users WHERE age BETWEEN 18 AND 65",
        "SELECT name FROM users WHERE email IS NULL",
        "SELECT name FROM users WHERE email IS NOT NULL",
        "SELECT name, CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END FROM users",
        "SELECT name, CASE age WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM users",
        "SELECT * FROM users",
        "SELECT COUNT(*) FROM users",
        "SELECT MAX(age) FROM users",
        "SELECT users.id, users.name FROM users",
        "SELECT name FROM users WHERE id = 1 AND age > 18 AND status = 'active'",
        "SELECT name FROM users WHERE category LIKE 'test%'",
        // INSERT statements
        "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
        "INSERT INTO users VALUES ('John', 'john@example.com')",
        "INSERT INTO users (name, email) VALUES ('John', 'john@example.com'), ('Jane', 'jane@example.com')",
        "INSERT INTO users (id, name) VALUES (1, 'John')",
        // UPDATE statements
        "UPDATE users SET name = 'John Doe' WHERE id = 1",
        "UPDATE users SET active = 1",
        "UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1",
        "UPDATE users SET count = 5 WHERE status = 'active'",
        // DELETE statements
        "DELETE FROM users WHERE id = 1",
        "DELETE FROM users",
        "DELETE FROM users WHERE age > 100",
        // CREATE TABLE statements
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)",
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY)",
        "CREATE TABLE users (id INTEGER PRIMARY KEY, active INTEGER DEFAULT 1)",
        "CREATE TABLE products (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE)",
        "CREATE TABLE items (id INTEGER, name TEXT NULL)",
        "CREATE TABLE test (id INTEGER)",
        // ALTER TABLE statements
        "ALTER TABLE users ADD COLUMN phone TEXT",
        "ALTER TABLE users ADD COLUMN age INTEGER NOT NULL",
        "ALTER TABLE users RENAME TO customers",
        "ALTER TABLE users RENAME COLUMN email TO email_address",
        "ALTER TABLE users DROP COLUMN phone",
        // CREATE INDEX statements
        "CREATE INDEX idx_email ON users (email)",
        "CREATE UNIQUE INDEX idx_email ON users (email)",
        "CREATE INDEX IF NOT EXISTS idx_email ON users (email)",
        "CREATE INDEX idx_name_email ON users (name, email)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_unique ON users (id)",
        // PRAGMA statements
        "PRAGMA database_list",
        "PRAGMA foreign_keys = ON",
        "PRAGMA foreign_keys = OFF",
        "PRAGMA reshardTable('users', 10)",
        "PRAGMA reshardTable(products, 5)"
    ];
    describe("round-trip generation", () => {
        it.each(testCases)("%s", (sql) => {
            const ast = parse(sql);
            const generated = generate(ast);
            expect(generated).toBe(sql);
        });
    });
    describe("Placeholders", () => {
        it("should generate SELECT with placeholders", () => {
            const sql = "SELECT * FROM users WHERE id = ? AND name = ?";
            const ast = parse(sql);
            const generated = generate(ast);
            expect(generated).toBe(sql);
        });
        it("should generate INSERT with placeholders", () => {
            const sql = "INSERT INTO users (name, age) VALUES (?, ?)";
            const ast = parse(sql);
            const generated = generate(ast);
            expect(generated).toBe(sql);
        });
        it("should generate UPDATE with placeholders", () => {
            const sql = "UPDATE users SET name = ?, age = ? WHERE id = ?";
            const ast = parse(sql);
            const generated = generate(ast);
            expect(generated).toBe(sql);
        });
        it("should generate DELETE with placeholder", () => {
            const sql = "DELETE FROM users WHERE id = ?";
            const ast = parse(sql);
            const generated = generate(ast);
            expect(generated).toBe(sql);
        });
        it("should generate complex query with multiple placeholders", () => {
            const sql = "SELECT name, email FROM users WHERE age > ? AND status = ? ORDER BY name ASC LIMIT ?";
            const ast = parse(sql);
            const generated = generate(ast);
            expect(generated).toBe(sql);
        });
        it("should generate UPDATE with RETURNING *", () => {
            const sql = "UPDATE users SET name = ? WHERE id = ? RETURNING *";
            const ast = parse(sql);
            const generated = generate(ast);
            expect(generated).toBe(sql);
        });
        it("should generate UPDATE with RETURNING specific columns", () => {
            const sql = "UPDATE users SET name = ? WHERE id = ? RETURNING id, name, email";
            const ast = parse(sql);
            const generated = generate(ast);
            expect(generated).toBe(sql);
        });
        it("should generate DELETE with RETURNING *", () => {
            const sql = "DELETE FROM users WHERE id = ? RETURNING *";
            const ast = parse(sql);
            const generated = generate(ast);
            expect(generated).toBe(sql);
        });
        it("should generate DELETE with RETURNING specific columns", () => {
            const sql = "DELETE FROM users WHERE id = ? RETURNING id, email";
            const ast = parse(sql);
            const generated = generate(ast);
            expect(generated).toBe(sql);
        });
    });
});
