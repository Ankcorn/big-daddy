export const testQueries = [
	// INSERT statements
	"INSERT INTO users (name, email, age) VALUES ('John Doe', 'john@example.com', 30);",
	"INSERT INTO products (id, name, price, category_id) VALUES (1, 'Laptop', 999.99, 2), (2, 'Mouse', 25.50, 3);",
	"INSERT INTO employees (name, department, salary, hire_date, is_active) VALUES ('Alice Smith', 'Engineering', 85000.00, '2024-03-15', 1), ('Bob Johnson', 'Marketing', 62000.50, '2024-01-20', 1), ('Carol Wilson', 'HR', 58000.75, '2023-11-10', 0), ('David Brown', 'Engineering', 92000.00, '2024-02-05', 1);",

	// CREATE TABLE statements
	"CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, email TEXT UNIQUE, created_at DATETIME DEFAULT CURRENT_TIMESTAMP);",
	"CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id), total DECIMAL(10,2), status TEXT CHECK(status IN ('pending', 'completed', 'cancelled')));",

	// ALTER statement
	"ALTER TABLE users ADD COLUMN phone_number TEXT;",

	// SELECT statements
	"SELECT u.name, u.email, COUNT(o.id) as order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.created_at > '2024-01-01' GROUP BY u.id HAVING COUNT(o.id) > 0 ORDER BY order_count DESC LIMIT 10;",
	"SELECT * FROM products WHERE price BETWEEN 10.00 AND 100.00 AND category_id IN (SELECT id FROM categories WHERE name LIKE '%electronics%');",

	// UPDATE statement
	"UPDATE users SET email = 'newemail@example.com', phone_number = '+1234567890' WHERE id = 1 AND name = 'John Doe';",

	// Multi-query statement (2 selects)
	"SELECT COUNT(*) FROM users; SELECT AVG(price) FROM products;",

	// Very complicated multi-query
	`WITH RECURSIVE category_tree AS (
    SELECT id, name, parent_id, 0 as level 
    FROM categories 
    WHERE parent_id IS NULL
    UNION ALL
    SELECT c.id, c.name, c.parent_id, ct.level + 1
    FROM categories c
    JOIN category_tree ct ON c.parent_id = ct.id
  ),
  user_stats AS (
    SELECT 
      u.id,
      u.name,
      COUNT(DISTINCT o.id) as total_orders,
      SUM(o.total) as total_spent,
      AVG(o.total) as avg_order_value,
      MAX(o.created_at) as last_order_date
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.created_at >= datetime('now', '-1 year')
    GROUP BY u.id, u.name
  )
  SELECT 
    ct.name as category_name,
    ct.level,
    COUNT(DISTINCT p.id) as product_count,
    AVG(p.price) as avg_price,
    SUM(oi.quantity * oi.price) as total_revenue,
    COUNT(DISTINCT us.id) as unique_customers
  FROM category_tree ct
  LEFT JOIN products p ON p.category_id = ct.id
  LEFT JOIN order_items oi ON oi.product_id = p.id
  LEFT JOIN orders o ON o.id = oi.order_id
  LEFT JOIN user_stats us ON us.id = o.user_id
  WHERE ct.level <= 3
  GROUP BY ct.id, ct.name, ct.level
  HAVING total_revenue > 1000
  ORDER BY total_revenue DESC, ct.level ASC;
  
  UPDATE user_preferences 
  SET theme = CASE 
    WHEN last_login < datetime('now', '-30 days') THEN 'dark'
    WHEN total_orders > 10 THEN 'premium'
    ELSE 'default'
  END
  WHERE user_id IN (
    SELECT u.id 
    FROM users u 
    JOIN user_stats us ON u.id = us.id 
    WHERE us.total_orders > 0
  );
  
  INSERT INTO audit_log (table_name, operation, user_id, timestamp, details)
  SELECT 
    'user_preferences' as table_name,
    'bulk_update' as operation,
    NULL as user_id,
    datetime('now') as timestamp,
    json_object(
      'affected_rows', changes(),
      'operation_type', 'theme_update',
      'criteria', 'activity_based'
    ) as details;`,
];
