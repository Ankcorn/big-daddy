import { describe, it, expect } from "vitest";
import { tokenize } from "./tokenizer";
describe("tokenizer", () => {
    describe("INSERT statements", () => {
        it("should tokenize complex multi-row INSERT statement", () => {
            const query = "INSERT INTO employees (name, department, salary, hire_date, is_active) VALUES ('Alice Smith', 'Engineering', 85000.00, '2024-03-15', 1), ('Bob Johnson', 'Marketing', 62000.50, '2024-01-20', 1), ('Carol Wilson', 'HR', 58000.75, '2023-11-10', 0), ('David Brown', 'Engineering', 92000.00, '2024-02-05', 1);";
            const tokens = tokenize(query);
            expect(tokens).toEqual([
                { token: "INSERT", type: "keyword", start: 0, end: 6 },
                { token: "INTO", type: "keyword", start: 7, end: 11 },
                { token: "employees", type: "identifier", start: 12, end: 21 },
                { token: "(", type: "punctuation", start: 22, end: 23 },
                { token: "name", type: "identifier", start: 23, end: 27 },
                { token: ",", type: "punctuation", start: 27, end: 28 },
                { token: "department", type: "identifier", start: 29, end: 39 },
                { token: ",", type: "punctuation", start: 39, end: 40 },
                { token: "salary", type: "identifier", start: 41, end: 47 },
                { token: ",", type: "punctuation", start: 47, end: 48 },
                { token: "hire_date", type: "identifier", start: 49, end: 58 },
                { token: ",", type: "punctuation", start: 58, end: 59 },
                { token: "is_active", type: "identifier", start: 60, end: 69 },
                { token: ")", type: "punctuation", start: 69, end: 70 },
                { token: "VALUES", type: "keyword", start: 71, end: 77 },
                { token: "(", type: "punctuation", start: 78, end: 79 },
                { token: "'Alice Smith'", type: "string", start: 79, end: 92 },
                { token: ",", type: "punctuation", start: 92, end: 93 },
                { token: "'Engineering'", type: "string", start: 94, end: 107 },
                { token: ",", type: "punctuation", start: 107, end: 108 },
                { token: "85000.00", type: "number", start: 109, end: 117 },
                { token: ",", type: "punctuation", start: 117, end: 118 },
                { token: "'2024-03-15'", type: "string", start: 119, end: 131 },
                { token: ",", type: "punctuation", start: 131, end: 132 },
                { token: "1", type: "number", start: 133, end: 134 },
                { token: ")", type: "punctuation", start: 134, end: 135 },
                { token: ",", type: "punctuation", start: 135, end: 136 },
                { token: "(", type: "punctuation", start: 137, end: 138 },
                { token: "'Bob Johnson'", type: "string", start: 138, end: 151 },
                { token: ",", type: "punctuation", start: 151, end: 152 },
                { token: "'Marketing'", type: "string", start: 153, end: 164 },
                { token: ",", type: "punctuation", start: 164, end: 165 },
                { token: "62000.50", type: "number", start: 166, end: 174 },
                { token: ",", type: "punctuation", start: 174, end: 175 },
                { token: "'2024-01-20'", type: "string", start: 176, end: 188 },
                { token: ",", type: "punctuation", start: 188, end: 189 },
                { token: "1", type: "number", start: 190, end: 191 },
                { token: ")", type: "punctuation", start: 191, end: 192 },
                { token: ",", type: "punctuation", start: 192, end: 193 },
                { token: "(", type: "punctuation", start: 194, end: 195 },
                { token: "'Carol Wilson'", type: "string", start: 195, end: 209 },
                { token: ",", type: "punctuation", start: 209, end: 210 },
                { token: "'HR'", type: "string", start: 211, end: 215 },
                { token: ",", type: "punctuation", start: 215, end: 216 },
                { token: "58000.75", type: "number", start: 217, end: 225 },
                { token: ",", type: "punctuation", start: 225, end: 226 },
                { token: "'2023-11-10'", type: "string", start: 227, end: 239 },
                { token: ",", type: "punctuation", start: 239, end: 240 },
                { token: "0", type: "number", start: 241, end: 242 },
                { token: ")", type: "punctuation", start: 242, end: 243 },
                { token: ",", type: "punctuation", start: 243, end: 244 },
                { token: "(", type: "punctuation", start: 245, end: 246 },
                { token: "'David Brown'", type: "string", start: 246, end: 259 },
                { token: ",", type: "punctuation", start: 259, end: 260 },
                { token: "'Engineering'", type: "string", start: 261, end: 274 },
                { token: ",", type: "punctuation", start: 274, end: 275 },
                { token: "92000.00", type: "number", start: 276, end: 284 },
                { token: ",", type: "punctuation", start: 284, end: 285 },
                { token: "'2024-02-05'", type: "string", start: 286, end: 298 },
                { token: ",", type: "punctuation", start: 298, end: 299 },
                { token: "1", type: "number", start: 300, end: 301 },
                { token: ")", type: "punctuation", start: 301, end: 302 },
                { token: ";", type: "punctuation", start: 302, end: 303 },
            ]);
        });
    });
    describe("CREATE TABLE statements", () => {
        it("should tokenize basic CREATE TABLE statement", () => {
            const query = "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, email TEXT UNIQUE, created_at DATETIME DEFAULT CURRENT_TIMESTAMP);";
            const tokens = tokenize(query);
            expect(tokens).toEqual([
                { token: "CREATE", type: "keyword", start: 0, end: 6 },
                { token: "TABLE", type: "keyword", start: 7, end: 12 },
                { token: "users", type: "identifier", start: 13, end: 18 },
                { token: "(", type: "punctuation", start: 19, end: 20 },
                { token: "id", type: "identifier", start: 20, end: 22 },
                { token: "INTEGER", type: "keyword", start: 23, end: 30 },
                { token: "PRIMARY", type: "keyword", start: 31, end: 38 },
                { token: "KEY", type: "keyword", start: 39, end: 42 },
                { token: "AUTOINCREMENT", type: "keyword", start: 43, end: 56 },
                { token: ",", type: "punctuation", start: 56, end: 57 },
                { token: "name", type: "identifier", start: 58, end: 62 },
                { token: "TEXT", type: "keyword", start: 63, end: 67 },
                { token: "NOT", type: "keyword", start: 68, end: 71 },
                { token: "NULL", type: "keyword", start: 72, end: 76 },
                { token: ",", type: "punctuation", start: 76, end: 77 },
                { token: "email", type: "identifier", start: 78, end: 83 },
                { token: "TEXT", type: "keyword", start: 84, end: 88 },
                { token: "UNIQUE", type: "keyword", start: 89, end: 95 },
                { token: ",", type: "punctuation", start: 95, end: 96 },
                { token: "created_at", type: "identifier", start: 97, end: 107 },
                { token: "DATETIME", type: "keyword", start: 108, end: 116 },
                { token: "DEFAULT", type: "keyword", start: 117, end: 124 },
                { token: "CURRENT_TIMESTAMP", type: "keyword", start: 125, end: 142 },
                { token: ")", type: "punctuation", start: 142, end: 143 },
                { token: ";", type: "punctuation", start: 143, end: 144 },
            ]);
        });
        it("should tokenize CREATE TABLE IF NOT EXISTS statement", () => {
            const query = "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id), total DECIMAL(10,2), status TEXT CHECK(status IN ('pending', 'completed', 'cancelled')));";
            const tokens = tokenize(query);
            expect(tokens).toEqual([
                { token: "CREATE", type: "keyword", start: 0, end: 6 },
                { token: "TABLE", type: "keyword", start: 7, end: 12 },
                { token: "IF", type: "keyword", start: 13, end: 15 },
                { token: "NOT", type: "keyword", start: 16, end: 19 },
                { token: "EXISTS", type: "keyword", start: 20, end: 26 },
                { token: "orders", type: "identifier", start: 27, end: 33 },
                { token: "(", type: "punctuation", start: 34, end: 35 },
                { token: "id", type: "identifier", start: 35, end: 37 },
                { token: "INTEGER", type: "keyword", start: 38, end: 45 },
                { token: "PRIMARY", type: "keyword", start: 46, end: 53 },
                { token: "KEY", type: "keyword", start: 54, end: 57 },
                { token: ",", type: "punctuation", start: 57, end: 58 },
                { token: "user_id", type: "identifier", start: 59, end: 66 },
                { token: "INTEGER", type: "keyword", start: 67, end: 74 },
                { token: "REFERENCES", type: "keyword", start: 75, end: 85 },
                { token: "users", type: "identifier", start: 86, end: 91 },
                { token: "(", type: "punctuation", start: 91, end: 92 },
                { token: "id", type: "identifier", start: 92, end: 94 },
                { token: ")", type: "punctuation", start: 94, end: 95 },
                { token: ",", type: "punctuation", start: 95, end: 96 },
                { token: "total", type: "identifier", start: 97, end: 102 },
                { token: "DECIMAL", type: "keyword", start: 103, end: 110 },
                { token: "(", type: "punctuation", start: 110, end: 111 },
                { token: "10", type: "number", start: 111, end: 113 },
                { token: ",", type: "punctuation", start: 113, end: 114 },
                { token: "2", type: "number", start: 114, end: 115 },
                { token: ")", type: "punctuation", start: 115, end: 116 },
                { token: ",", type: "punctuation", start: 116, end: 117 },
                { token: "status", type: "identifier", start: 118, end: 124 },
                { token: "TEXT", type: "keyword", start: 125, end: 129 },
                { token: "CHECK", type: "keyword", start: 130, end: 135 },
                { token: "(", type: "punctuation", start: 135, end: 136 },
                { token: "status", type: "identifier", start: 136, end: 142 },
                { token: "IN", type: "keyword", start: 143, end: 145 },
                { token: "(", type: "punctuation", start: 146, end: 147 },
                { token: "'pending'", type: "string", start: 147, end: 156 },
                { token: ",", type: "punctuation", start: 156, end: 157 },
                { token: "'completed'", type: "string", start: 158, end: 169 },
                { token: ",", type: "punctuation", start: 169, end: 170 },
                { token: "'cancelled'", type: "string", start: 171, end: 182 },
                { token: ")", type: "punctuation", start: 182, end: 183 },
                { token: ")", type: "punctuation", start: 183, end: 184 },
                { token: ")", type: "punctuation", start: 184, end: 185 },
                { token: ";", type: "punctuation", start: 185, end: 186 },
            ]);
        });
    });
    describe("ALTER statements", () => {
        it("should tokenize ALTER TABLE ADD COLUMN statement", () => {
            const query = "ALTER TABLE users ADD COLUMN phone_number TEXT;";
            const tokens = tokenize(query);
            expect(tokens).toEqual([
                { token: "ALTER", type: "keyword", start: 0, end: 5 },
                { token: "TABLE", type: "keyword", start: 6, end: 11 },
                { token: "users", type: "identifier", start: 12, end: 17 },
                { token: "ADD", type: "keyword", start: 18, end: 21 },
                { token: "COLUMN", type: "keyword", start: 22, end: 28 },
                { token: "phone_number", type: "identifier", start: 29, end: 41 },
                { token: "TEXT", type: "keyword", start: 42, end: 46 },
                { token: ";", type: "punctuation", start: 46, end: 47 },
            ]);
        });
    });
    describe("SELECT statements", () => {
        it("should tokenize complex SELECT with JOINs and aggregates", () => {
            const query = "SELECT u.name, u.email, COUNT(o.id) as order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.created_at > '2024-01-01' GROUP BY u.id HAVING COUNT(o.id) > 0 ORDER BY order_count DESC LIMIT 10;";
            const tokens = tokenize(query);
            expect(tokens).toEqual([
                { token: "SELECT", type: "keyword", start: 0, end: 6 },
                { token: "u", type: "identifier", start: 7, end: 8 },
                { token: ".", type: "punctuation", start: 8, end: 9 },
                { token: "name", type: "identifier", start: 9, end: 13 },
                { token: ",", type: "punctuation", start: 13, end: 14 },
                { token: "u", type: "identifier", start: 15, end: 16 },
                { token: ".", type: "punctuation", start: 16, end: 17 },
                { token: "email", type: "identifier", start: 17, end: 22 },
                { token: ",", type: "punctuation", start: 22, end: 23 },
                { token: "COUNT", type: "function", start: 24, end: 29 },
                { token: "(", type: "punctuation", start: 29, end: 30 },
                { token: "o", type: "identifier", start: 30, end: 31 },
                { token: ".", type: "punctuation", start: 31, end: 32 },
                { token: "id", type: "identifier", start: 32, end: 34 },
                { token: ")", type: "punctuation", start: 34, end: 35 },
                { token: "as", type: "keyword", start: 36, end: 38 },
                { token: "order_count", type: "identifier", start: 39, end: 50 },
                { token: "FROM", type: "keyword", start: 51, end: 55 },
                { token: "users", type: "identifier", start: 56, end: 61 },
                { token: "u", type: "identifier", start: 62, end: 63 },
                { token: "LEFT", type: "keyword", start: 64, end: 68 },
                { token: "JOIN", type: "keyword", start: 69, end: 73 },
                { token: "orders", type: "identifier", start: 74, end: 80 },
                { token: "o", type: "identifier", start: 81, end: 82 },
                { token: "ON", type: "keyword", start: 83, end: 85 },
                { token: "u", type: "identifier", start: 86, end: 87 },
                { token: ".", type: "punctuation", start: 87, end: 88 },
                { token: "id", type: "identifier", start: 88, end: 90 },
                { token: "=", type: "operator", start: 91, end: 92 },
                { token: "o", type: "identifier", start: 93, end: 94 },
                { token: ".", type: "punctuation", start: 94, end: 95 },
                { token: "user_id", type: "identifier", start: 95, end: 102 },
                { token: "WHERE", type: "keyword", start: 103, end: 108 },
                { token: "u", type: "identifier", start: 109, end: 110 },
                { token: ".", type: "punctuation", start: 110, end: 111 },
                { token: "created_at", type: "identifier", start: 111, end: 121 },
                { token: ">", type: "operator", start: 122, end: 123 },
                { token: "'2024-01-01'", type: "string", start: 124, end: 136 },
                { token: "GROUP", type: "keyword", start: 137, end: 142 },
                { token: "BY", type: "keyword", start: 143, end: 145 },
                { token: "u", type: "identifier", start: 146, end: 147 },
                { token: ".", type: "punctuation", start: 147, end: 148 },
                { token: "id", type: "identifier", start: 148, end: 150 },
                { token: "HAVING", type: "keyword", start: 151, end: 157 },
                { token: "COUNT", type: "function", start: 158, end: 163 },
                { token: "(", type: "punctuation", start: 163, end: 164 },
                { token: "o", type: "identifier", start: 164, end: 165 },
                { token: ".", type: "punctuation", start: 165, end: 166 },
                { token: "id", type: "identifier", start: 166, end: 168 },
                { token: ")", type: "punctuation", start: 168, end: 169 },
                { token: ">", type: "operator", start: 170, end: 171 },
                { token: "0", type: "number", start: 172, end: 173 },
                { token: "ORDER", type: "keyword", start: 174, end: 179 },
                { token: "BY", type: "keyword", start: 180, end: 182 },
                { token: "order_count", type: "identifier", start: 183, end: 194 },
                { token: "DESC", type: "keyword", start: 195, end: 199 },
                { token: "LIMIT", type: "keyword", start: 200, end: 205 },
                { token: "10", type: "number", start: 206, end: 208 },
                { token: ";", type: "punctuation", start: 208, end: 209 },
            ]);
        });
        it("should tokenize SELECT with subquery and BETWEEN", () => {
            const query = "SELECT * FROM products WHERE price BETWEEN 10.00 AND 100.00 AND category_id IN (SELECT id FROM categories WHERE name LIKE '%electronics%');";
            const tokens = tokenize(query);
            expect(tokens).toEqual([
                { token: "SELECT", type: "keyword", start: 0, end: 6 },
                { token: "*", type: "operator", start: 7, end: 8 },
                { token: "FROM", type: "keyword", start: 9, end: 13 },
                { token: "products", type: "identifier", start: 14, end: 22 },
                { token: "WHERE", type: "keyword", start: 23, end: 28 },
                { token: "price", type: "identifier", start: 29, end: 34 },
                { token: "BETWEEN", type: "keyword", start: 35, end: 42 },
                { token: "10.00", type: "number", start: 43, end: 48 },
                { token: "AND", type: "keyword", start: 49, end: 52 },
                { token: "100.00", type: "number", start: 53, end: 59 },
                { token: "AND", type: "keyword", start: 60, end: 63 },
                { token: "category_id", type: "identifier", start: 64, end: 75 },
                { token: "IN", type: "keyword", start: 76, end: 78 },
                { token: "(", type: "punctuation", start: 79, end: 80 },
                { token: "SELECT", type: "keyword", start: 80, end: 86 },
                { token: "id", type: "identifier", start: 87, end: 89 },
                { token: "FROM", type: "keyword", start: 90, end: 94 },
                { token: "categories", type: "identifier", start: 95, end: 105 },
                { token: "WHERE", type: "keyword", start: 106, end: 111 },
                { token: "name", type: "identifier", start: 112, end: 116 },
                { token: "LIKE", type: "keyword", start: 117, end: 121 },
                { token: "'%electronics%'", type: "string", start: 122, end: 137 },
                { token: ")", type: "punctuation", start: 137, end: 138 },
                { token: ";", type: "punctuation", start: 138, end: 139 },
            ]);
        });
    });
    describe("UPDATE statements", () => {
        it("should tokenize UPDATE with SET and WHERE", () => {
            const query = "UPDATE users SET email = 'newemail@example.com', phone_number = '+1234567890' WHERE id = 1 AND name = 'John Doe';";
            const tokens = tokenize(query);
            expect(tokens).toEqual([
                { token: "UPDATE", type: "keyword", start: 0, end: 6 },
                { token: "users", type: "identifier", start: 7, end: 12 },
                { token: "SET", type: "keyword", start: 13, end: 16 },
                { token: "email", type: "identifier", start: 17, end: 22 },
                { token: "=", type: "operator", start: 23, end: 24 },
                { token: "'newemail@example.com'", type: "string", start: 25, end: 47 },
                { token: ",", type: "punctuation", start: 47, end: 48 },
                { token: "phone_number", type: "identifier", start: 49, end: 61 },
                { token: "=", type: "operator", start: 62, end: 63 },
                { token: "'+1234567890'", type: "string", start: 64, end: 77 },
                { token: "WHERE", type: "keyword", start: 78, end: 83 },
                { token: "id", type: "identifier", start: 84, end: 86 },
                { token: "=", type: "operator", start: 87, end: 88 },
                { token: "1", type: "number", start: 89, end: 90 },
                { token: "AND", type: "keyword", start: 91, end: 94 },
                { token: "name", type: "identifier", start: 95, end: 99 },
                { token: "=", type: "operator", start: 100, end: 101 },
                { token: "'John Doe'", type: "string", start: 102, end: 112 },
                { token: ";", type: "punctuation", start: 112, end: 113 },
            ]);
        });
    });
    describe("Multi-query statements", () => {
        it("should tokenize simple multi-query with semicolon separator", () => {
            const query = "SELECT COUNT(*) FROM users; SELECT AVG(price) FROM products;";
            const tokens = tokenize(query);
            expect(tokens).toEqual([
                { token: "SELECT", type: "keyword", start: 0, end: 6 },
                { token: "COUNT", type: "function", start: 7, end: 12 },
                { token: "(", type: "punctuation", start: 12, end: 13 },
                { token: "*", type: "operator", start: 13, end: 14 },
                { token: ")", type: "punctuation", start: 14, end: 15 },
                { token: "FROM", type: "keyword", start: 16, end: 20 },
                { token: "users", type: "identifier", start: 21, end: 26 },
                { token: ";", type: "punctuation", start: 26, end: 27 },
                { token: "SELECT", type: "keyword", start: 28, end: 34 },
                { token: "AVG", type: "function", start: 35, end: 38 },
                { token: "(", type: "punctuation", start: 38, end: 39 },
                { token: "price", type: "identifier", start: 39, end: 44 },
                { token: ")", type: "punctuation", start: 44, end: 45 },
                { token: "FROM", type: "keyword", start: 46, end: 50 },
                { token: "products", type: "identifier", start: 51, end: 59 },
                { token: ";", type: "punctuation", start: 59, end: 60 },
            ]);
        });
    });
    describe("SQLite Functions", () => {
        it("should tokenize nested function calls", () => {
            const query = "SELECT UPPER(SUBSTR(TRIM(name), 1, LENGTH(TRIM(name)) / 2)) FROM users WHERE LENGTH(REPLACE(email, '.', '')) > 10;";
            const tokens = tokenize(query);
            expect(tokens).toEqual([
                { token: "SELECT", type: "keyword", start: 0, end: 6 },
                { token: "UPPER", type: "function", start: 7, end: 12 },
                { token: "(", type: "punctuation", start: 12, end: 13 },
                { token: "SUBSTR", type: "function", start: 13, end: 19 },
                { token: "(", type: "punctuation", start: 19, end: 20 },
                { token: "TRIM", type: "function", start: 20, end: 24 },
                { token: "(", type: "punctuation", start: 24, end: 25 },
                { token: "name", type: "identifier", start: 25, end: 29 },
                { token: ")", type: "punctuation", start: 29, end: 30 },
                { token: ",", type: "punctuation", start: 30, end: 31 },
                { token: "1", type: "number", start: 32, end: 33 },
                { token: ",", type: "punctuation", start: 33, end: 34 },
                { token: "LENGTH", type: "function", start: 35, end: 41 },
                { token: "(", type: "punctuation", start: 41, end: 42 },
                { token: "TRIM", type: "function", start: 42, end: 46 },
                { token: "(", type: "punctuation", start: 46, end: 47 },
                { token: "name", type: "identifier", start: 47, end: 51 },
                { token: ")", type: "punctuation", start: 51, end: 52 },
                { token: ")", type: "punctuation", start: 52, end: 53 },
                { token: "/", type: "operator", start: 54, end: 55 },
                { token: "2", type: "number", start: 56, end: 57 },
                { token: ")", type: "punctuation", start: 57, end: 58 },
                { token: ")", type: "punctuation", start: 58, end: 59 },
                { token: "FROM", type: "keyword", start: 60, end: 64 },
                { token: "users", type: "identifier", start: 65, end: 70 },
                { token: "WHERE", type: "keyword", start: 71, end: 76 },
                { token: "LENGTH", type: "function", start: 77, end: 83 },
                { token: "(", type: "punctuation", start: 83, end: 84 },
                { token: "REPLACE", type: "function", start: 84, end: 91 },
                { token: "(", type: "punctuation", start: 91, end: 92 },
                { token: "email", type: "identifier", start: 92, end: 97 },
                { token: ",", type: "punctuation", start: 97, end: 98 },
                { token: "'.'", type: "string", start: 99, end: 102 },
                { token: ",", type: "punctuation", start: 102, end: 103 },
                { token: "''", type: "string", start: 104, end: 106 },
                { token: ")", type: "punctuation", start: 106, end: 107 },
                { token: ")", type: "punctuation", start: 107, end: 108 },
                { token: ">", type: "operator", start: 109, end: 110 },
                { token: "10", type: "number", start: 111, end: 113 },
                { token: ";", type: "punctuation", start: 113, end: 114 },
            ]);
        });
    });
    describe("Advanced Tokenization Features", () => {
        describe("Escaped quotes in strings", () => {
            it("should handle SQL-style escaped quotes (double single quotes)", () => {
                const query = "INSERT INTO users (name, note) VALUES ('John Doe', 'He said ''Hello'' to me');";
                const tokens = tokenize(query);
                expect(tokens).toEqual([
                    { token: "INSERT", type: "keyword", start: 0, end: 6 },
                    { token: "INTO", type: "keyword", start: 7, end: 11 },
                    { token: "users", type: "identifier", start: 12, end: 17 },
                    { token: "(", type: "punctuation", start: 18, end: 19 },
                    { token: "name", type: "identifier", start: 19, end: 23 },
                    { token: ",", type: "punctuation", start: 23, end: 24 },
                    { token: "note", type: "identifier", start: 25, end: 29 },
                    { token: ")", type: "punctuation", start: 29, end: 30 },
                    { token: "VALUES", type: "keyword", start: 31, end: 37 },
                    { token: "(", type: "punctuation", start: 38, end: 39 },
                    { token: "'John Doe'", type: "string", start: 39, end: 49 },
                    { token: ",", type: "punctuation", start: 49, end: 50 },
                    { token: "'He said ''Hello'' to me'", type: "string", start: 51, end: 76 },
                    { token: ")", type: "punctuation", start: 76, end: 77 },
                    { token: ";", type: "punctuation", start: 77, end: 78 },
                ]);
            });
            it("should handle backslash escaped quotes", () => {
                const query = "SELECT * FROM posts WHERE content = 'can\\'t stop';";
                const tokens = tokenize(query);
                expect(tokens).toEqual([
                    { token: "SELECT", type: "keyword", start: 0, end: 6 },
                    { token: "*", type: "operator", start: 7, end: 8 },
                    { token: "FROM", type: "keyword", start: 9, end: 13 },
                    { token: "posts", type: "identifier", start: 14, end: 19 },
                    { token: "WHERE", type: "keyword", start: 20, end: 25 },
                    { token: "content", type: "identifier", start: 26, end: 33 },
                    { token: "=", type: "operator", start: 34, end: 35 },
                    { token: "'can\\'t stop'", type: "string", start: 36, end: 49 },
                    { token: ";", type: "punctuation", start: 49, end: 50 },
                ]);
            });
        });
        describe("Different quote types", () => {
            it("should handle mixed quote types", () => {
                const query = `SELECT \`user\`.name, "order"."total price" FROM users WHERE note = 'special';`;
                const tokens = tokenize(query);
                expect(tokens).toEqual([
                    { token: "SELECT", type: "keyword", start: 0, end: 6 },
                    { token: "`user`", type: "identifier", start: 7, end: 13 },
                    { token: ".", type: "punctuation", start: 13, end: 14 },
                    { token: "name", type: "identifier", start: 14, end: 18 },
                    { token: ",", type: "punctuation", start: 18, end: 19 },
                    { token: '"order"', type: "identifier", start: 20, end: 27 },
                    { token: ".", type: "punctuation", start: 27, end: 28 },
                    { token: '"total price"', type: "identifier", start: 28, end: 41 },
                    { token: "FROM", type: "keyword", start: 42, end: 46 },
                    { token: "users", type: "identifier", start: 47, end: 52 },
                    { token: "WHERE", type: "keyword", start: 53, end: 58 },
                    { token: "note", type: "identifier", start: 59, end: 63 },
                    { token: "=", type: "operator", start: 64, end: 65 },
                    { token: "'special'", type: "string", start: 66, end: 75 },
                    { token: ";", type: "punctuation", start: 75, end: 76 },
                ]);
            });
        });
        describe("Comments", () => {
            it("should handle nested and complex comments", () => {
                const query = `-- Start query
        SELECT name /* user's name */, email -- user email
        FROM users; /*
        multi-line comment
        with -- embedded single line comment
        */`;
                const tokens = tokenize(query);
                expect(tokens).toEqual([
                    { token: "SELECT", type: "keyword", start: 23, end: 29 },
                    { token: "name", type: "identifier", start: 30, end: 34 },
                    { token: ",", type: "punctuation", start: 52, end: 53 },
                    { token: "email", type: "identifier", start: 54, end: 59 },
                    { token: "FROM", type: "keyword", start: 82, end: 86 },
                    { token: "users", type: "identifier", start: 87, end: 92 },
                    { token: ";", type: "punctuation", start: 92, end: 93 },
                ]);
            });
        });
        describe("Number formats", () => {
            it("should handle mixed number formats", () => {
                const query = "SELECT 42, 3.14, 0xFF, 0b1010, 1.5e10, users.id FROM mixed_numbers;";
                const tokens = tokenize(query);
                expect(tokens).toEqual([
                    { token: "SELECT", type: "keyword", start: 0, end: 6 },
                    { token: "42", type: "number", start: 7, end: 9 },
                    { token: ",", type: "punctuation", start: 9, end: 10 },
                    { token: "3.14", type: "number", start: 11, end: 15 },
                    { token: ",", type: "punctuation", start: 15, end: 16 },
                    { token: "0xFF", type: "number", start: 17, end: 21 },
                    { token: ",", type: "punctuation", start: 21, end: 22 },
                    { token: "0b1010", type: "number", start: 23, end: 29 },
                    { token: ",", type: "punctuation", start: 29, end: 30 },
                    { token: "1.5e10", type: "number", start: 31, end: 37 },
                    { token: ",", type: "punctuation", start: 37, end: 38 },
                    { token: "users", type: "identifier", start: 39, end: 44 },
                    { token: ".", type: "punctuation", start: 44, end: 45 },
                    { token: "id", type: "identifier", start: 45, end: 47 },
                    { token: "FROM", type: "keyword", start: 48, end: 52 },
                    { token: "mixed_numbers", type: "identifier", start: 53, end: 66 },
                    { token: ";", type: "punctuation", start: 66, end: 67 },
                ]);
            });
        });
        describe("Complex combinations", () => {
            it("should handle escaped quotes with comments and special numbers", () => {
                const query = `-- Complex query example
        SELECT 'user\\'s name' as name, /* user identifier */ 0xFF as hex_id
        FROM "user table" WHERE "special field" = 'can\\'t escape this' -- tricky string
        AND score > 1.5e3; /* scientific notation */`;
                const tokens = tokenize(query);
                expect(tokens).toEqual([
                    { token: "SELECT", type: "keyword", start: 33, end: 39 },
                    { token: "'user\\'s name'", type: "string", start: 40, end: 54 },
                    { token: "as", type: "keyword", start: 55, end: 57 },
                    { token: "name", type: "identifier", start: 58, end: 62 },
                    { token: ",", type: "punctuation", start: 62, end: 63 },
                    { token: "0xFF", type: "number", start: 86, end: 90 },
                    { token: "as", type: "keyword", start: 91, end: 93 },
                    { token: "hex_id", type: "identifier", start: 94, end: 100 },
                    { token: "FROM", type: "keyword", start: 109, end: 113 },
                    { token: '"user table"', type: "identifier", start: 114, end: 126 },
                    { token: "WHERE", type: "keyword", start: 127, end: 132 },
                    { token: '"special field"', type: "identifier", start: 133, end: 148 },
                    { token: "=", type: "operator", start: 149, end: 150 },
                    { token: "'can\\'t escape this'", type: "string", start: 151, end: 171 },
                    { token: "AND", type: "keyword", start: 197, end: 200 },
                    { token: "score", type: "identifier", start: 201, end: 206 },
                    { token: ">", type: "operator", start: 207, end: 208 },
                    { token: "1.5e3", type: "number", start: 209, end: 214 },
                    { token: ";", type: "punctuation", start: 214, end: 215 },
                ]);
            });
        });
    });
    describe("Error Handling", () => {
        describe("Unterminated strings", () => {
            it("should provide clear error message for unterminated string", () => {
                const query = "SELECT 'hello world FROM users;";
                const expectedError = `Unterminated string literal at line 1, column 8
SELECT 'hello world FROM users;
       ^`;
                expect(() => tokenize(query)).toThrow(expectedError);
            });
            it("should provide clear error message for unterminated identifier", () => {
                const query = 'SELECT "user name FROM users;';
                const expectedError = `Unterminated identifier at line 1, column 8
SELECT "user name FROM users;
       ^`;
                expect(() => tokenize(query)).toThrow(expectedError);
            });
            it("should handle multi-line SQL with proper line/column tracking", () => {
                const query = `SELECT name
FROM users
WHERE email = 'unclosed string
AND active = 1;`;
                const expectedError = `Unterminated string literal at line 3, column 15
WHERE email = 'unclosed string
              ^`;
                expect(() => tokenize(query)).toThrow(expectedError);
            });
        });
    });
    describe("Placeholders", () => {
        it("should tokenize query with placeholders", () => {
            const query = "SELECT * FROM users WHERE id = ? AND name = ?";
            const tokens = tokenize(query);
            expect(tokens).toEqual([
                { token: "SELECT", type: "keyword", start: 0, end: 6 },
                { token: "*", type: "operator", start: 7, end: 8 },
                { token: "FROM", type: "keyword", start: 9, end: 13 },
                { token: "users", type: "identifier", start: 14, end: 19 },
                { token: "WHERE", type: "keyword", start: 20, end: 25 },
                { token: "id", type: "identifier", start: 26, end: 28 },
                { token: "=", type: "operator", start: 29, end: 30 },
                { token: "?", type: "placeholder", start: 31, end: 32 },
                { token: "AND", type: "keyword", start: 33, end: 36 },
                { token: "name", type: "identifier", start: 37, end: 41 },
                { token: "=", type: "operator", start: 42, end: 43 },
                { token: "?", type: "placeholder", start: 44, end: 45 },
            ]);
        });
        it("should tokenize INSERT with placeholders", () => {
            const query = "INSERT INTO users (name, age) VALUES (?, ?)";
            const tokens = tokenize(query);
            expect(tokens).toContainEqual({ token: "?", type: "placeholder", start: 38, end: 39 });
            expect(tokens).toContainEqual({ token: "?", type: "placeholder", start: 41, end: 42 });
        });
        it("should tokenize UPDATE with placeholders", () => {
            const query = "UPDATE users SET name = ?, age = ? WHERE id = ?";
            const tokens = tokenize(query);
            const placeholders = tokens.filter(t => t.type === "placeholder");
            expect(placeholders).toHaveLength(3);
        });
    });
});
