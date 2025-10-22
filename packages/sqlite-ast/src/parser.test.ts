import { describe, it, expect } from "vitest";
import { parse } from "./parser";
import type { SelectStatement } from "./types";

describe("parser", () => {
  describe("SELECT statements", () => {
    it("should parse a simple SELECT statement", () => {
      const sql = "SELECT name, email FROM users;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "SelectStatement",
        select: [
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "name" },
          },
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "email" },
          },
        ],
        from: {
          type: "Identifier",
          name: "users",
        },
      });
    });

    it("should parse SELECT with WHERE clause", () => {
      const sql = "SELECT name, email FROM users WHERE id = 1;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "SelectStatement",
        select: [
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "name" },
          },
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "email" },
          },
        ],
        from: {
          type: "Identifier",
          name: "users",
        },
        where: {
          type: "BinaryExpression",
          operator: "=",
          left: { type: "Identifier", name: "id" },
          right: { type: "Literal", value: 1, raw: "1" },
        },
      });
    });

    it("should parse SELECT with WHERE clause using string comparison", () => {
      const sql =
        "SELECT id, name FROM products WHERE category = 'electronics';";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "SelectStatement",
        select: [
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "id" },
          },
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "name" },
          },
        ],
        from: {
          type: "Identifier",
          name: "products",
        },
        where: {
          type: "BinaryExpression",
          operator: "=",
          left: { type: "Identifier", name: "category" },
          right: {
            type: "Literal",
            value: "electronics",
            raw: "'electronics'",
          },
        },
      });
    });

    it("should parse SELECT with JOIN", () => {
      const sql =
        "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "SelectStatement",
        select: [
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "users.name" },
          },
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "orders.total" },
          },
        ],
        from: {
          type: "Identifier",
          name: "users",
        },
        joins: [
          {
            type: "JoinClause",
            joinType: "JOIN",
            table: { type: "Identifier", name: "orders" },
            on: {
              type: "BinaryExpression",
              operator: "=",
              left: { type: "Identifier", name: "users.id" },
              right: { type: "Identifier", name: "orders.user_id" },
            },
          },
        ],
      });
    });

    it("should parse SELECT with column aliases", () => {
      const sql = "SELECT name AS user_name, email AS user_email FROM users;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "SelectStatement",
        select: [
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "name" },
            alias: { type: "Identifier", name: "user_name" },
          },
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "email" },
            alias: { type: "Identifier", name: "user_email" },
          },
        ],
        from: {
          type: "Identifier",
          name: "users",
        },
      });
    });

    it("should parse SELECT with ORDER BY", () => {
      const sql =
        "SELECT name, created_at FROM users ORDER BY created_at DESC, name ASC;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "SelectStatement",
        select: [
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "name" },
          },
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "created_at" },
          },
        ],
        from: {
          type: "Identifier",
          name: "users",
        },
        orderBy: [
          {
            type: "OrderByClause",
            expression: { type: "Identifier", name: "created_at" },
            direction: "DESC",
          },
          {
            type: "OrderByClause",
            expression: { type: "Identifier", name: "name" },
            direction: "ASC",
          },
        ],
      });
    });

    it("should parse SELECT with LIMIT and OFFSET", () => {
      const sql = "SELECT name FROM users LIMIT 10 OFFSET 20;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "SelectStatement",
        select: [
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "name" },
          },
        ],
        from: {
          type: "Identifier",
          name: "users",
        },
        limit: { type: "Literal", value: 10, raw: "10" },
        offset: { type: "Literal", value: 20, raw: "20" },
      });
    });

    it("should parse SELECT with function calls", () => {
      const sql = "SELECT COUNT(*), MAX(price) FROM products;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "SelectStatement",
        select: [
          {
            type: "SelectClause",
            expression: {
              type: "FunctionCall",
              name: "COUNT",
              arguments: [{ type: "Identifier", name: "*" }],
            },
          },
          {
            type: "SelectClause",
            expression: {
              type: "FunctionCall",
              name: "MAX",
              arguments: [{ type: "Identifier", name: "price" }],
            },
          },
        ],
        from: {
          type: "Identifier",
          name: "products",
        },
      });
    });

    it("should parse SELECT with wildcard", () => {
      const sql = "SELECT * FROM users;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "SelectStatement",
        select: [
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "*" },
          },
        ],
        from: {
          type: "Identifier",
          name: "users",
        },
      });
    });

    it("should parse SELECT with GROUP BY and HAVING", () => {
      const sql =
        "SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 5;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "SelectStatement",
        select: [
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "category" },
          },
          {
            type: "SelectClause",
            expression: {
              type: "FunctionCall",
              name: "COUNT",
              arguments: [{ type: "Identifier", name: "*" }],
            },
          },
        ],
        from: {
          type: "Identifier",
          name: "products",
        },
        groupBy: [{ type: "Identifier", name: "category" }],
        having: {
          type: "BinaryExpression",
          operator: ">",
          left: {
            type: "FunctionCall",
            name: "COUNT",
            arguments: [{ type: "Identifier", name: "*" }],
          },
          right: { type: "Literal", value: 5, raw: "5" },
        },
      });
    });

    it("should parse WHERE with AND/OR logical operators", () => {
      const sql =
        "SELECT name FROM users WHERE age > 18 AND status = 'active';";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "SelectStatement",
        select: [
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "name" },
          },
        ],
        from: {
          type: "Identifier",
          name: "users",
        },
        where: {
          type: "BinaryExpression",
          operator: "AND",
          left: {
            type: "BinaryExpression",
            operator: ">",
            left: { type: "Identifier", name: "age" },
            right: { type: "Literal", value: 18, raw: "18" },
          },
          right: {
            type: "BinaryExpression",
            operator: "=",
            left: { type: "Identifier", name: "status" },
            right: { type: "Literal", value: "active", raw: "'active'" },
          },
        },
      });
    });

    it("should parse SELECT DISTINCT", () => {
      const sql = "SELECT DISTINCT category FROM products;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "SelectStatement",
        distinct: true,
        select: [
          {
            type: "SelectClause",
            expression: { type: "Identifier", name: "category" },
          },
        ],
        from: {
          type: "Identifier",
          name: "products",
        },
      });
    });

    it("should parse subquery in IN clause", () => {
      const sql =
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders);";
      const ast = parse(sql) as SelectStatement;

      expect(ast.where).toEqual({
        type: "InExpression",
        expression: { type: "Identifier", name: "id" },
        values: [
          {
            type: "SubqueryExpression",
            query: {
              type: "SelectStatement",
              select: [
                {
                  type: "SelectClause",
                  expression: { type: "Identifier", name: "user_id" },
                },
              ],
              from: { type: "Identifier", name: "orders" },
            },
          },
        ],
      });
    });

    it("should parse CASE expression", () => {
      const sql =
        "SELECT CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END FROM users;";
      const ast = parse(sql) as SelectStatement;

      expect(ast.select[0]?.expression).toEqual({
        type: "CaseExpression",
        whenClauses: [
          {
            when: {
              type: "BinaryExpression",
              operator: "<",
              left: { type: "Identifier", name: "age" },
              right: { type: "Literal", value: 18, raw: "18" },
            },
            then: { type: "Literal", value: "minor", raw: "'minor'" },
          },
        ],
        else: { type: "Literal", value: "adult", raw: "'adult'" },
      });
    });

    it("should parse all comparison operators", () => {
      const testCases = [
        { sql: "SELECT * FROM items WHERE price < 100;", operator: "<" },
        { sql: "SELECT * FROM items WHERE price <= 100;", operator: "<=" },
        { sql: "SELECT * FROM items WHERE price > 100;", operator: ">" },
        { sql: "SELECT * FROM items WHERE price >= 100;", operator: ">=" },
        { sql: "SELECT * FROM items WHERE price != 100;", operator: "!=" },
        { sql: "SELECT * FROM items WHERE price <> 100;", operator: "<>" },
      ];

      testCases.forEach(({ sql, operator }) => {
        const ast = parse(sql) as SelectStatement;
        expect(ast.where).toEqual({
          type: "BinaryExpression",
          operator,
          left: { type: "Identifier", name: "price" },
          right: { type: "Literal", value: 100, raw: "100" },
        });
      });
    });

    it("should parse IN operator", () => {
      const sql = "SELECT * FROM users WHERE id IN (1, 2, 3);";
      const ast = parse(sql) as SelectStatement;

      expect(ast.where).toEqual({
        type: "InExpression",
        expression: { type: "Identifier", name: "id" },
        values: [
          { type: "Literal", value: 1, raw: "1" },
          { type: "Literal", value: 2, raw: "2" },
          { type: "Literal", value: 3, raw: "3" },
        ],
      });
    });

    it("should parse BETWEEN operator", () => {
      const sql = "SELECT * FROM products WHERE price BETWEEN 10 AND 100;";
      const ast = parse(sql) as SelectStatement;

      expect(ast.where).toEqual({
        type: "BetweenExpression",
        expression: { type: "Identifier", name: "price" },
        lower: { type: "Literal", value: 10, raw: "10" },
        upper: { type: "Literal", value: 100, raw: "100" },
      });
    });

    it("should parse LIKE operator", () => {
      const sql = "SELECT * FROM users WHERE name LIKE '%smith%';";
      const ast = parse(sql) as SelectStatement;

      expect(ast.where).toEqual({
        type: "BinaryExpression",
        operator: "LIKE",
        left: { type: "Identifier", name: "name" },
        right: { type: "Literal", value: "%smith%", raw: "'%smith%'" },
      });
    });

    it("should parse IS NULL", () => {
      const sql = "SELECT * FROM users WHERE deleted_at IS NULL;";
      const ast = parse(sql) as SelectStatement;

      expect(ast.where).toEqual({
        type: "UnaryExpression",
        operator: "IS NULL",
        expression: { type: "Identifier", name: "deleted_at" },
      });
    });

    it("should parse IS NOT NULL", () => {
      const sql = "SELECT * FROM users WHERE email IS NOT NULL;";
      const ast = parse(sql) as SelectStatement;

      expect(ast.where).toEqual({
        type: "UnaryExpression",
        operator: "IS NOT NULL",
        expression: { type: "Identifier", name: "email" },
      });
    });

    it("should parse parenthesized expressions", () => {
      const sql =
        "SELECT * FROM users WHERE (age > 18 AND country = 'US') OR admin = 1;";
      const ast = parse(sql) as SelectStatement;

      expect(ast.where).toEqual({
        type: "BinaryExpression",
        operator: "OR",
        left: {
          type: "BinaryExpression",
          operator: "AND",
          left: {
            type: "BinaryExpression",
            operator: ">",
            left: { type: "Identifier", name: "age" },
            right: { type: "Literal", value: 18, raw: "18" },
          },
          right: {
            type: "BinaryExpression",
            operator: "=",
            left: { type: "Identifier", name: "country" },
            right: { type: "Literal", value: "US", raw: "'US'" },
          },
        },
        right: {
          type: "BinaryExpression",
          operator: "=",
          left: { type: "Identifier", name: "admin" },
          right: { type: "Literal", value: 1, raw: "1" },
        },
      });
    });
  });

  describe("INSERT statements", () => {
    it("should parse INSERT with column names", () => {
      const sql =
        "INSERT INTO users (name, email) VALUES ('John', 'john@example.com');";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "InsertStatement",
        table: { type: "Identifier", name: "users" },
        columns: [
          { type: "Identifier", name: "name" },
          { type: "Identifier", name: "email" },
        ],
        values: [
          [
            { type: "Literal", value: "John", raw: "'John'" },
            {
              type: "Literal",
              value: "john@example.com",
              raw: "'john@example.com'",
            },
          ],
        ],
      });
    });
  });

  describe("UPDATE statements", () => {
    it("should parse UPDATE with WHERE clause", () => {
      const sql = "UPDATE users SET name = 'John', age = 25 WHERE id = 1;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "UpdateStatement",
        table: { type: "Identifier", name: "users" },
        set: [
          {
            column: { type: "Identifier", name: "name" },
            value: { type: "Literal", value: "John", raw: "'John'" },
          },
          {
            column: { type: "Identifier", name: "age" },
            value: { type: "Literal", value: 25, raw: "25" },
          },
        ],
        where: {
          type: "BinaryExpression",
          operator: "=",
          left: { type: "Identifier", name: "id" },
          right: { type: "Literal", value: 1, raw: "1" },
        },
      });
    });
  });

  describe("DELETE statements", () => {
    it("should parse DELETE with WHERE clause", () => {
      const sql = "DELETE FROM users WHERE id = 1;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "DeleteStatement",
        table: { type: "Identifier", name: "users" },
        where: {
          type: "BinaryExpression",
          operator: "=",
          left: { type: "Identifier", name: "id" },
          right: { type: "Literal", value: 1, raw: "1" },
        },
      });
    });
  });

  describe("CREATE TABLE statements", () => {
    it("should parse CREATE TABLE with column definitions", () => {
      const sql =
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT);";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "CreateTableStatement",
        table: { type: "Identifier", name: "users" },
        columns: [
          {
            type: "ColumnDefinition",
            name: { type: "Identifier", name: "id" },
            dataType: "INTEGER",
            constraints: [
              {
                type: "ColumnConstraint",
                constraint: "PRIMARY KEY",
              },
            ],
          },
          {
            type: "ColumnDefinition",
            name: { type: "Identifier", name: "name" },
            dataType: "TEXT",
            constraints: [
              {
                type: "ColumnConstraint",
                constraint: "NOT NULL",
              },
            ],
          },
          {
            type: "ColumnDefinition",
            name: { type: "Identifier", name: "email" },
            dataType: "TEXT",
          },
        ],
      });
    });
  });

  describe("ALTER TABLE statements", () => {
    it("should parse ALTER TABLE ADD COLUMN", () => {
      const sql = "ALTER TABLE users ADD COLUMN phone TEXT;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "AlterTableStatement",
        table: { type: "Identifier", name: "users" },
        action: "ADD COLUMN",
        column: {
          type: "ColumnDefinition",
          name: { type: "Identifier", name: "phone" },
          dataType: "TEXT",
        },
      });
    });

    it("should parse ALTER TABLE RENAME TO", () => {
      const sql = "ALTER TABLE users RENAME TO customers;";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "AlterTableStatement",
        table: { type: "Identifier", name: "users" },
        action: "RENAME TO",
        newName: { type: "Identifier", name: "customers" },
      });
    });
  });

  describe("CREATE INDEX statements", () => {
    it("should parse CREATE INDEX", () => {
      const sql = "CREATE INDEX idx_email ON users (email);";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "CreateIndexStatement",
        name: { type: "Identifier", name: "idx_email" },
        table: { type: "Identifier", name: "users" },
        columns: [{ type: "Identifier", name: "email" }],
      });
    });

    it("should parse CREATE UNIQUE INDEX", () => {
      const sql = "CREATE UNIQUE INDEX idx_email ON users (email);";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "CreateIndexStatement",
        unique: true,
        name: { type: "Identifier", name: "idx_email" },
        table: { type: "Identifier", name: "users" },
        columns: [{ type: "Identifier", name: "email" }],
      });
    });

    it("should parse CREATE INDEX with multiple columns", () => {
      const sql = "CREATE INDEX idx_name_email ON users (name, email);";
      const ast = parse(sql);

      expect(ast).toEqual({
        type: "CreateIndexStatement",
        name: { type: "Identifier", name: "idx_name_email" },
        table: { type: "Identifier", name: "users" },
        columns: [
          { type: "Identifier", name: "name" },
          { type: "Identifier", name: "email" },
        ],
      });
    });
  });

  describe("Placeholders", () => {
    it("should parse SELECT with placeholders", () => {
      const sql = "SELECT * FROM users WHERE id = ? AND name = ?";
      const ast = parse(sql);

      expect(ast.type).toBe("SelectStatement");
      const selectStmt = ast as SelectStatement;

      expect(selectStmt.where).toEqual({
        type: "BinaryExpression",
        operator: "AND",
        left: {
          type: "BinaryExpression",
          operator: "=",
          left: { type: "Identifier", name: "id" },
          right: { type: "Placeholder", parameterIndex: 0 },
        },
        right: {
          type: "BinaryExpression",
          operator: "=",
          left: { type: "Identifier", name: "name" },
          right: { type: "Placeholder", parameterIndex: 1 },
        },
      });
    });

    it("should parse INSERT with placeholders", () => {
      const sql = "INSERT INTO users (name, age) VALUES (?, ?)";
      const ast = parse(sql);

      expect(ast.type).toBe("InsertStatement");
      const insertStmt = ast as InsertStatement;

      expect(insertStmt.values).toEqual([
        [
          { type: "Placeholder", parameterIndex: 0 },
          { type: "Placeholder", parameterIndex: 1 },
        ],
      ]);
    });

    it("should parse UPDATE with placeholders", () => {
      const sql = "UPDATE users SET name = ?, age = ? WHERE id = ?";
      const ast = parse(sql);

      expect(ast.type).toBe("UpdateStatement");
      const updateStmt = ast as UpdateStatement;

      expect(updateStmt.set).toEqual([
        {
          column: { type: "Identifier", name: "name" },
          value: { type: "Placeholder", parameterIndex: 0 },
        },
        {
          column: { type: "Identifier", name: "age" },
          value: { type: "Placeholder", parameterIndex: 1 },
        },
      ]);

      expect(updateStmt.where).toEqual({
        type: "BinaryExpression",
        operator: "=",
        left: { type: "Identifier", name: "id" },
        right: { type: "Placeholder", parameterIndex: 2 },
      });
    });

    it("should parse DELETE with placeholder", () => {
      const sql = "DELETE FROM users WHERE id = ?";
      const ast = parse(sql);

      expect(ast.type).toBe("DeleteStatement");
      const deleteStmt = ast as DeleteStatement;

      expect(deleteStmt.where).toEqual({
        type: "BinaryExpression",
        operator: "=",
        left: { type: "Identifier", name: "id" },
        right: { type: "Placeholder", parameterIndex: 0 },
      });
    });
  });
});
