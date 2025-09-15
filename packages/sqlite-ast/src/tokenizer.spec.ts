import { describe, it, expect } from 'vitest'
import { tokenize } from './tokenizer'

describe('tokenizer', () => {
  describe('INSERT statements', () => {
    it('should tokenize simple INSERT statement', () => {
      const query = "INSERT INTO users (name, email, age) VALUES ('John Doe', 'john@example.com', 30);"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'INSERT', 'INTO', 'users', '(', 'name', ',', 'email', ',', 'age', ')', 
        'VALUES', '(', "'John Doe'", ',', "'john@example.com'", ',', '30', ')', ';'
      ])
    })

    it('should tokenize multi-value INSERT statement', () => {
      const query = "INSERT INTO products (id, name, price, category_id) VALUES (1, 'Laptop', 999.99, 2), (2, 'Mouse', 25.50, 3);"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'INSERT', 'INTO', 'products', '(', 'id', ',', 'name', ',', 'price', ',', 'category_id', ')', 
        'VALUES', 
        '(', '1', ',', "'Laptop'", ',', '999.99', ',', '2', ')', 
        ',', 
        '(', '2', ',', "'Mouse'", ',', '25.50', ',', '3', ')', 
        ';'
      ])
    })

    it('should tokenize complex multi-row INSERT statement', () => {
      const query = "INSERT INTO employees (name, department, salary, hire_date, is_active) VALUES ('Alice Smith', 'Engineering', 85000.00, '2024-03-15', 1), ('Bob Johnson', 'Marketing', 62000.50, '2024-01-20', 1), ('Carol Wilson', 'HR', 58000.75, '2023-11-10', 0), ('David Brown', 'Engineering', 92000.00, '2024-02-05', 1);"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'INSERT', 'INTO', 'employees', 
        '(', 'name', ',', 'department', ',', 'salary', ',', 'hire_date', ',', 'is_active', ')', 
        'VALUES',
        '(', "'Alice Smith'", ',', "'Engineering'", ',', '85000.00', ',', "'2024-03-15'", ',', '1', ')',
        ',',
        '(', "'Bob Johnson'", ',', "'Marketing'", ',', '62000.50', ',', "'2024-01-20'", ',', '1', ')',
        ',',
        '(', "'Carol Wilson'", ',', "'HR'", ',', '58000.75', ',', "'2023-11-10'", ',', '0', ')',
        ',',
        '(', "'David Brown'", ',', "'Engineering'", ',', '92000.00', ',', "'2024-02-05'", ',', '1', ')',
        ';'
      ])
    })
  })

  describe('CREATE TABLE statements', () => {
    it('should tokenize basic CREATE TABLE statement', () => {
      const query = "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, email TEXT UNIQUE, created_at DATETIME DEFAULT CURRENT_TIMESTAMP);"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'CREATE', 'TABLE', 'users',
        '(',
        'id', 'INTEGER', 'PRIMARY', 'KEY', 'AUTOINCREMENT', ',',
        'name', 'TEXT', 'NOT', 'NULL', ',',
        'email', 'TEXT', 'UNIQUE', ',',
        'created_at', 'DATETIME', 'DEFAULT', 'CURRENT_TIMESTAMP',
        ')',
        ';'
      ])
    })

    it('should tokenize CREATE TABLE IF NOT EXISTS statement', () => {
      const query = "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id), total DECIMAL(10,2), status TEXT CHECK(status IN ('pending', 'completed', 'cancelled')));"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'CREATE', 'TABLE', 'IF', 'NOT', 'EXISTS', 'orders',
        '(',
        'id', 'INTEGER', 'PRIMARY', 'KEY', ',',
        'user_id', 'INTEGER', 'REFERENCES', 'users', '(', 'id', ')', ',',
        'total', 'DECIMAL', '(', '10', ',', '2', ')', ',',
        'status', 'TEXT', 'CHECK', '(', 'status', 'IN', '(', "'pending'", ',', "'completed'", ',', "'cancelled'", ')', ')',
        ')',
        ';'
      ])
    })
  })

  describe('ALTER statements', () => {
    it('should tokenize ALTER TABLE ADD COLUMN statement', () => {
      const query = "ALTER TABLE users ADD COLUMN phone_number TEXT;"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'ALTER', 'TABLE', 'users', 'ADD', 'COLUMN', 'phone_number', 'TEXT', ';'
      ])
    })
  })

  describe('SELECT statements', () => {
    it('should tokenize complex SELECT with JOINs and aggregates', () => {
      const query = "SELECT u.name, u.email, COUNT(o.id) as order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.created_at > '2024-01-01' GROUP BY u.id HAVING COUNT(o.id) > 0 ORDER BY order_count DESC LIMIT 10;"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'SELECT', 
        'u', '.', 'name', ',', 
        'u', '.', 'email', ',', 
        'COUNT', '(', 'o', '.', 'id', ')', 'as', 'order_count',
        'FROM', 'users', 'u',
        'LEFT', 'JOIN', 'orders', 'o', 'ON', 'u', '.', 'id', '=', 'o', '.', 'user_id',
        'WHERE', 'u', '.', 'created_at', '>', "'2024-01-01'",
        'GROUP', 'BY', 'u', '.', 'id',
        'HAVING', 'COUNT', '(', 'o', '.', 'id', ')', '>', '0',
        'ORDER', 'BY', 'order_count', 'DESC',
        'LIMIT', '10',
        ';'
      ])
    })

    it('should tokenize SELECT with subquery and BETWEEN', () => {
      const query = "SELECT * FROM products WHERE price BETWEEN 10.00 AND 100.00 AND category_id IN (SELECT id FROM categories WHERE name LIKE '%electronics%');"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'SELECT', '*', 
        'FROM', 'products', 
        'WHERE', 'price', 'BETWEEN', '10.00', 'AND', '100.00', 
        'AND', 'category_id', 'IN', 
        '(', 'SELECT', 'id', 'FROM', 'categories', 'WHERE', 'name', 'LIKE', "'%electronics%'", ')', 
        ';'
      ])
    })
  })

  describe('UPDATE statements', () => {
    it('should tokenize UPDATE with SET and WHERE', () => {
      const query = "UPDATE users SET email = 'newemail@example.com', phone_number = '+1234567890' WHERE id = 1 AND name = 'John Doe';"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'UPDATE', 'users', 
        'SET', 
        'email', '=', "'newemail@example.com'", ',', 
        'phone_number', '=', "'+1234567890'", 
        'WHERE', 'id', '=', '1', 'AND', 'name', '=', "'John Doe'", 
        ';'
      ])
    })
  })

  describe('Multi-query statements', () => {
    it('should tokenize simple multi-query with semicolon separator', () => {
      const query = "SELECT COUNT(*) FROM users; SELECT AVG(price) FROM products;"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'SELECT', 'COUNT', '(', '*', ')', 'FROM', 'users', ';',
        'SELECT', 'AVG', '(', 'price', ')', 'FROM', 'products', ';'
      ])
    })

    it('should tokenize complex multi-query with CTEs', () => {
      const query = `WITH RECURSIVE category_tree AS (
        SELECT id, name, parent_id, 0 as level 
        FROM categories 
        WHERE parent_id IS NULL
        UNION ALL
        SELECT c.id, c.name, c.parent_id, ct.level + 1
        FROM categories c
        JOIN category_tree ct ON c.parent_id = ct.id
      )
      SELECT ct.name as category_name, ct.level
      FROM category_tree ct
      WHERE ct.level <= 3
      ORDER BY ct.level ASC;
      
      UPDATE user_preferences 
      SET theme = CASE 
        WHEN last_login < datetime('now', '-30 days') THEN 'dark'
        ELSE 'default'
      END;`
      
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'WITH', 'RECURSIVE', 'category_tree', 'AS', '(',
        'SELECT', 'id', ',', 'name', ',', 'parent_id', ',', '0', 'as', 'level',
        'FROM', 'categories',
        'WHERE', 'parent_id', 'IS', 'NULL',
        'UNION', 'ALL',
        'SELECT', 'c', '.', 'id', ',', 'c', '.', 'name', ',', 'c', '.', 'parent_id', ',', 'ct', '.', 'level', '+', '1',
        'FROM', 'categories', 'c',
        'JOIN', 'category_tree', 'ct', 'ON', 'c', '.', 'parent_id', '=', 'ct', '.', 'id',
        ')',
        'SELECT', 'ct', '.', 'name', 'as', 'category_name', ',', 'ct', '.', 'level',
        'FROM', 'category_tree', 'ct',
        'WHERE', 'ct', '.', 'level', '<=', '3',
        'ORDER', 'BY', 'ct', '.', 'level', 'ASC', ';',
        'UPDATE', 'user_preferences',
        'SET', 'theme', '=', 'CASE',
        'WHEN', 'last_login', '<', 'datetime', '(', "'now'", ',', "'-30 days'", ')', 'THEN', "'dark'",
        'ELSE', "'default'",
        'END', ';'
      ])
    })
  })

  describe('SQLite Functions', () => {
    it('should tokenize string functions', () => {
      const query = "SELECT LENGTH(name), UPPER(email), LOWER(title), SUBSTR(description, 1, 100), TRIM(content), REPLACE(text, 'old', 'new') FROM articles;"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'SELECT', 
        'LENGTH', '(', 'name', ')', ',',
        'UPPER', '(', 'email', ')', ',',
        'LOWER', '(', 'title', ')', ',',
        'SUBSTR', '(', 'description', ',', '1', ',', '100', ')', ',',
        'TRIM', '(', 'content', ')', ',',
        'REPLACE', '(', 'text', ',', "'old'", ',', "'new'", ')',
        'FROM', 'articles', ';'
      ])
    })

    it('should tokenize aggregate functions', () => {
      const query = "SELECT COUNT(*), COUNT(DISTINCT category_id), SUM(price), AVG(rating), MIN(created_at), MAX(updated_at), GROUP_CONCAT(tags, ', ') FROM products GROUP BY category_id;"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'SELECT', 
        'COUNT', '(', '*', ')', ',',
        'COUNT', '(', 'DISTINCT', 'category_id', ')', ',',
        'SUM', '(', 'price', ')', ',',
        'AVG', '(', 'rating', ')', ',',
        'MIN', '(', 'created_at', ')', ',',
        'MAX', '(', 'updated_at', ')', ',',
        'GROUP_CONCAT', '(', 'tags', ',', "', '", ')',
        'FROM', 'products',
        'GROUP', 'BY', 'category_id', ';'
      ])
    })

    it('should tokenize date and time functions', () => {
      const query = "SELECT datetime('now'), date('now', '+7 days'), time('12:34:56'), strftime('%Y-%m-%d %H:%M:%S', created_at), julianday('2024-01-01') - julianday('2023-01-01') FROM events;"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'SELECT',
        'datetime', '(', "'now'", ')', ',',
        'date', '(', "'now'", ',', "'+7 days'", ')', ',',
        'time', '(', "'12:34:56'", ')', ',',
        'strftime', '(', "'%Y-%m-%d %H:%M:%S'", ',', 'created_at', ')', ',',
        'julianday', '(', "'2024-01-01'", ')', '-', 'julianday', '(', "'2023-01-01'", ')',
        'FROM', 'events', ';'
      ])
    })

    it('should tokenize numeric functions', () => {
      const query = "SELECT ABS(-42), ROUND(price, 2), CAST(rating AS INTEGER), COALESCE(discount, 0.0), NULLIF(quantity, 0), RANDOM(), HEX(id) FROM products;"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'SELECT',
        'ABS', '(', '-', '42', ')', ',',
        'ROUND', '(', 'price', ',', '2', ')', ',',
        'CAST', '(', 'rating', 'AS', 'INTEGER', ')', ',',
        'COALESCE', '(', 'discount', ',', '0.0', ')', ',',
        'NULLIF', '(', 'quantity', ',', '0', ')', ',',
        'RANDOM', '(', ')', ',',
        'HEX', '(', 'id', ')',
        'FROM', 'products', ';'
      ])
    })

    it('should tokenize conditional functions', () => {
      const query = "SELECT IIF(price > 100, 'expensive', 'affordable'), CASE WHEN stock > 0 THEN 'available' ELSE 'out_of_stock' END, IFNULL(description, 'No description') FROM products;"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'SELECT',
        'IIF', '(', 'price', '>', '100', ',', "'expensive'", ',', "'affordable'", ')', ',',
        'CASE', 'WHEN', 'stock', '>', '0', 'THEN', "'available'", 'ELSE', "'out_of_stock'", 'END', ',',
        'IFNULL', '(', 'description', ',', "'No description'", ')',
        'FROM', 'products', ';'
      ])
    })

    it('should tokenize JSON functions', () => {
      const query = "SELECT json_extract(metadata, '$.title'), json_array_length(tags), json_valid(config), json_set(data, '$.updated', datetime('now')) FROM documents WHERE json_type(metadata, '$.version') = 'text';"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'SELECT',
        'json_extract', '(', 'metadata', ',', "'$.title'", ')', ',',
        'json_array_length', '(', 'tags', ')', ',',
        'json_valid', '(', 'config', ')', ',',
        'json_set', '(', 'data', ',', "'$.updated'", ',', 'datetime', '(', "'now'", ')', ')',
        'FROM', 'documents',
        'WHERE', 'json_type', '(', 'metadata', ',', "'$.version'", ')', '=', "'text'", ';'
      ])
    })
    it('should tokenize window functions', () => {
      const query = "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC), RANK() OVER (PARTITION BY department ORDER BY salary DESC), LAG(salary, 1) OVER (ORDER BY hire_date) FROM employees;"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'SELECT',
        'name', ',', 'salary', ',',
        'ROW_NUMBER', '(', ')', 'OVER', '(', 'ORDER', 'BY', 'salary', 'DESC', ')', ',',
        'RANK', '(', ')', 'OVER', '(', 'PARTITION', 'BY', 'department', 'ORDER', 'BY', 'salary', 'DESC', ')', ',',
        'LAG', '(', 'salary', ',', '1', ')', 'OVER', '(', 'ORDER', 'BY', 'hire_date', ')',
        'FROM', 'employees', ';'
      ])
    })

    it('should tokenize nested function calls', () => {
      const query = "SELECT UPPER(SUBSTR(TRIM(name), 1, LENGTH(TRIM(name)) / 2)) FROM users WHERE LENGTH(REPLACE(email, '.', '')) > 10;"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'SELECT',
        'UPPER', '(', 
        'SUBSTR', '(', 
        'TRIM', '(', 'name', ')', ',', '1', ',', 
        'LENGTH', '(', 'TRIM', '(', 'name', ')', ')', '/', '2',
        ')', 
        ')',
        'FROM', 'users',
        'WHERE', 'LENGTH', '(', 'REPLACE', '(', 'email', ',', "'.'", ',', "''", ')', ')', '>', '10', ';'
      ])
    })

    it('should tokenize mathematical functions', () => {
      const query = "SELECT SQRT(area), POWER(radius, 2) * 3.14159, SIN(angle), COS(angle), TAN(angle), LOG(value), EXP(rate) FROM measurements;"
      const tokens = tokenize(query)
      
      expect(tokens).toEqual([
        'SELECT',
        'SQRT', '(', 'area', ')', ',',
        'POWER', '(', 'radius', ',', '2', ')', '*', '3.14159', ',',
        'SIN', '(', 'angle', ')', ',',
        'COS', '(', 'angle', ')', ',',
        'TAN', '(', 'angle', ')', ',',
        'LOG', '(', 'value', ')', ',',
        'EXP', '(', 'rate', ')',
        'FROM', 'measurements', ';'
      ])
    })
  })

  describe('Advanced Tokenization Features', () => {
    describe('Escaped quotes in strings', () => {
      it('should handle SQL-style escaped quotes (double single quotes)', () => {
        const query = "INSERT INTO users (name, note) VALUES ('John Doe', 'He said ''Hello'' to me');"
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'INSERT', 'INTO', 'users', '(', 'name', ',', 'note', ')', 
          'VALUES', '(', "'John Doe'", ',', "'He said ''Hello'' to me'", ')', ';'
        ])
      })

      it('should handle backslash escaped quotes', () => {
        const query = "SELECT * FROM posts WHERE content = 'can\\'t stop';"
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'SELECT', '*', 'FROM', 'posts', 
          'WHERE', 'content', '=', "'can\\'t stop'", ';'
        ])
      })

      it('should handle mixed escaped quotes', () => {
        const query = "UPDATE messages SET text = 'It\\'s a ''great'' day!' WHERE id = 1;"
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'UPDATE', 'messages', 'SET', 'text', '=', "'It\\'s a ''great'' day!'", 
          'WHERE', 'id', '=', '1', ';'
        ])
      })
    })

    describe('Different quote types', () => {
      it('should handle double-quoted identifiers', () => {
        const query = 'SELECT "user name", "email address" FROM "user table";'
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'SELECT', '"user name"', ',', '"email address"', 
          'FROM', '"user table"', ';'
        ])
      })

      it('should handle backtick identifiers', () => {
        const query = 'SELECT `user name`, `email address` FROM `user table`;'
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'SELECT', '`user name`', ',', '`email address`', 
          'FROM', '`user table`', ';'
        ])
      })

      it('should handle mixed quote types', () => {
        const query = `SELECT \`user\`.name, "order"."total price" FROM users WHERE note = 'special';`
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'SELECT', '`user`', '.', 'name', ',', '"order"', '.', '"total price"',
          'FROM', 'users', 'WHERE', 'note', '=', "'special'", ';'
        ])
      })
    })

    describe('Comments', () => {
      it('should handle single-line comments', () => {
        const query = `SELECT name, email -- get user info
        FROM users WHERE active = 1; -- only active users`
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'SELECT', 'name', ',', 'email',
          'FROM', 'users', 'WHERE', 'active', '=', '1', ';'
        ])
      })

      it('should handle multi-line comments', () => {
        const query = `SELECT /* select user data */ name, 
        /* this is email */ email 
        FROM users /* user table */ WHERE active = 1;`
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'SELECT', 'name', ',', 'email',
          'FROM', 'users', 'WHERE', 'active', '=', '1', ';'
        ])
      })

      it('should handle nested and complex comments', () => {
        const query = `-- Start query
        SELECT name /* user's name */, email -- user email
        FROM users; /* 
        multi-line comment
        with -- embedded single line comment
        */`
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'SELECT', 'name', ',', 'email',
          'FROM', 'users', ';'
        ])
      })
    })

    describe('Number formats', () => {
      it('should handle hexadecimal literals', () => {
        const query = "SELECT 0x1A2B, 0xFF00, 0x0 FROM numbers;"
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'SELECT', '0x1A2B', ',', '0xFF00', ',', '0x0',
          'FROM', 'numbers', ';'
        ])
      })

      it('should handle binary literals', () => {
        const query = "SELECT 0b1010, 0b11111111, 0b0 FROM binary_data;"
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'SELECT', '0b1010', ',', '0b11111111', ',', '0b0',
          'FROM', 'binary_data', ';'
        ])
      })

      it('should handle scientific notation', () => {
        const query = "SELECT 1.5e10, 2E-3, 3.14159e+2, 1e5 FROM scientific;"
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'SELECT', '1.5e10', ',', '2E-3', ',', '3.14159e+2', ',', '1e5',
          'FROM', 'scientific', ';'
        ])
      })

      it('should handle mixed number formats', () => {
        const query = "SELECT 42, 3.14, 0xFF, 0b1010, 1.5e10, users.id FROM mixed_numbers;"
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'SELECT', '42', ',', '3.14', ',', '0xFF', ',', '0b1010', ',', '1.5e10', ',',
          'users', '.', 'id', 'FROM', 'mixed_numbers', ';'
        ])
      })
    })

    describe('Complex combinations', () => {
      it('should handle escaped quotes with comments and special numbers', () => {
        const query = `-- Complex query example
        SELECT 'user\\'s name' as name, /* user identifier */ 0xFF as hex_id
        FROM "user table" WHERE "special field" = 'can\\'t escape this' -- tricky string
        AND score > 1.5e3; /* scientific notation */`
        
        const tokens = tokenize(query)
        
        expect(tokens).toEqual([
          'SELECT', "'user\\'s name'", 'as', 'name', ',', '0xFF', 'as', 'hex_id',
          'FROM', '"user table"', 'WHERE', '"special field"', '=', "'can\\'t escape this'",
          'AND', 'score', '>', '1.5e3', ';'
        ])
      })
    })
  })

  describe('Error Handling', () => {
    describe('Unterminated strings', () => {
      it('should provide clear error message for unterminated string', () => {
        const query = "SELECT 'hello world FROM users;"
        
        const expectedError = `Unterminated string literal at line 1, column 8
SELECT 'hello world FROM users;
       ^`
        
        expect(() => tokenize(query)).toThrow(expectedError)
      })

      it('should provide clear error message for unterminated identifier', () => {
        const query = 'SELECT "user name FROM users;'
        
        const expectedError = `Unterminated identifier at line 1, column 8
SELECT "user name FROM users;
       ^`
        
        expect(() => tokenize(query)).toThrow(expectedError)
      })

      it('should handle multi-line SQL with proper line/column tracking', () => {
        const query = `SELECT name
FROM users
WHERE email = 'unclosed string
AND active = 1;`
        
        const expectedError = `Unterminated string literal at line 3, column 15
WHERE email = 'unclosed string
              ^`
        
        expect(() => tokenize(query)).toThrow(expectedError)
      })
    })

  })
})
