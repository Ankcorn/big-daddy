import type {
  Statement,
  SelectStatement,
  InsertStatement,
  UpdateStatement,
  DeleteStatement,
  CreateTableStatement,
  AlterTableStatement,
  CreateIndexStatement,
  PragmaStatement,
  DropTableStatement,
  Expression,
  Identifier,
  Literal,
  BinaryExpression,
  UnaryExpression,
  FunctionCall,
  InExpression,
  BetweenExpression,
  SubqueryExpression,
  CaseExpression,
  SelectClause,
  JoinClause,
  OrderByClause,
  ColumnDefinition,
  ColumnConstraint
} from "./types";

/**
 * Generate SQL from an AST node
 */
export function generate(ast: Statement): string {
  switch (ast.type) {
    case "SelectStatement":
      return generateSelect(ast);
    case "InsertStatement":
      return generateInsert(ast);
    case "UpdateStatement":
      return generateUpdate(ast);
    case "DeleteStatement":
      return generateDelete(ast);
    case "CreateTableStatement":
      return generateCreateTable(ast);
    case "AlterTableStatement":
      return generateAlterTable(ast);
    case "CreateIndexStatement":
      return generateCreateIndex(ast);
    case "PragmaStatement":
      return generatePragma(ast);
    case "DropTableStatement":
      return generateDropTable(ast);
    default:
      throw new Error(`Unknown statement type: ${(ast as any).type}`);
  }
}

function generateSelect(stmt: SelectStatement): string {
  let sql = "SELECT";

  if (stmt.distinct) {
    sql += " DISTINCT";
  }

  sql += " " + stmt.select.map(generateSelectClause).join(", ");

  if (stmt.from) {
    sql += " FROM " + generateIdentifier(stmt.from);
  }

  if (stmt.joins && stmt.joins.length > 0) {
    sql += " " + stmt.joins.map(generateJoin).join(" ");
  }

  if (stmt.where) {
    sql += " WHERE " + generateExpression(stmt.where);
  }

  if (stmt.groupBy && stmt.groupBy.length > 0) {
    sql += " GROUP BY " + stmt.groupBy.map(generateExpression).join(", ");
  }

  if (stmt.having) {
    sql += " HAVING " + generateExpression(stmt.having);
  }

  if (stmt.orderBy && stmt.orderBy.length > 0) {
    sql += " ORDER BY " + stmt.orderBy.map(generateOrderBy).join(", ");
  }

  if (stmt.limit) {
    sql += " LIMIT " + generateExpression(stmt.limit);
  }

  if (stmt.offset) {
    sql += " OFFSET " + generateExpression(stmt.offset);
  }

  return sql;
}

function generateSelectClause(clause: SelectClause): string {
  let sql = generateExpression(clause.expression);

  if (clause.alias) {
    sql += " AS " + generateIdentifier(clause.alias);
  }

  return sql;
}

function generateJoin(join: JoinClause): string {
  let sql = join.joinType + " " + generateIdentifier(join.table);

  if (join.on) {
    sql += " ON " + generateExpression(join.on);
  }

  return sql;
}

function generateOrderBy(orderBy: OrderByClause): string {
  let sql = generateExpression(orderBy.expression);

  if (orderBy.direction) {
    sql += " " + orderBy.direction;
  }

  return sql;
}

function generateInsert(stmt: InsertStatement): string {
  let sql = "INSERT INTO " + generateIdentifier(stmt.table);

  if (stmt.columns && stmt.columns.length > 0) {
    sql += " (" + stmt.columns.map(generateIdentifier).join(", ") + ")";
  }

  sql += " VALUES ";
  sql += stmt.values.map(row =>
    "(" + row.map(generateExpression).join(", ") + ")"
  ).join(", ");

  return sql;
}

function generateUpdate(stmt: UpdateStatement): string {
  let sql = "UPDATE " + generateIdentifier(stmt.table);

  sql += " SET " + stmt.set.map(s =>
    generateIdentifier(s.column) + " = " + generateExpression(s.value)
  ).join(", ");

  if (stmt.where) {
    sql += " WHERE " + generateExpression(stmt.where);
  }

  if (stmt.returning && stmt.returning.length > 0) {
    sql += " RETURNING " + stmt.returning.map(generateSelectClause).join(", ");
  }

  return sql;
}

function generateDelete(stmt: DeleteStatement): string {
  let sql = "DELETE FROM " + generateIdentifier(stmt.table);

  if (stmt.where) {
    sql += " WHERE " + generateExpression(stmt.where);
  }

  if (stmt.returning && stmt.returning.length > 0) {
    sql += " RETURNING " + stmt.returning.map(generateSelectClause).join(", ");
  }

  return sql;
}

function generateCreateTable(stmt: CreateTableStatement): string {
  let sql = "CREATE TABLE";

  if (stmt.ifNotExists) {
    sql += " IF NOT EXISTS";
  }

  sql += " " + generateIdentifier(stmt.table);
  sql += " (" + stmt.columns.map(generateColumnDefinition).join(", ") + ")";

  return sql;
}

function generateAlterTable(stmt: AlterTableStatement): string {
  let sql = "ALTER TABLE " + generateIdentifier(stmt.table);

  switch (stmt.action) {
    case "ADD COLUMN":
      if (!stmt.column) {
        throw new Error("ADD COLUMN requires a column definition");
      }
      sql += " ADD COLUMN " + generateColumnDefinition(stmt.column);
      break;
    case "RENAME TO":
      if (!stmt.newName) {
        throw new Error("RENAME TO requires a newName");
      }
      sql += " RENAME TO " + generateIdentifier(stmt.newName);
      break;
    case "RENAME COLUMN":
      if (!stmt.oldColumnName || !stmt.newName) {
        throw new Error("RENAME COLUMN requires oldColumnName and newName");
      }
      sql += " RENAME COLUMN " + generateIdentifier(stmt.oldColumnName) + " TO " + generateIdentifier(stmt.newName);
      break;
    case "DROP COLUMN":
      if (!stmt.oldColumnName) {
        throw new Error("DROP COLUMN requires oldColumnName");
      }
      sql += " DROP COLUMN " + generateIdentifier(stmt.oldColumnName);
      break;
  }

  return sql;
}

function generateCreateIndex(stmt: CreateIndexStatement): string {
  let sql = "CREATE";

  if (stmt.unique) {
    sql += " UNIQUE";
  }

  sql += " INDEX";

  if (stmt.ifNotExists) {
    sql += " IF NOT EXISTS";
  }

  sql += " " + generateIdentifier(stmt.name);
  sql += " ON " + generateIdentifier(stmt.table);
  sql += " (" + stmt.columns.map(generateIdentifier).join(", ") + ")";

  return sql;
}

function generateDropTable(stmt: DropTableStatement): string {
  let sql = "DROP TABLE";

  if (stmt.ifExists) {
    sql += " IF EXISTS";
  }

  sql += " " + generateIdentifier(stmt.table);

  return sql;
}

function generatePragma(stmt: PragmaStatement): string {
  let sql = "PRAGMA " + stmt.name;

  if (stmt.arguments && stmt.arguments.length > 0) {
    // Function-call style PRAGMA
    sql += "(" + stmt.arguments.map(generateExpression).join(", ") + ")";
  } else if (stmt.value) {
    // Key=value style PRAGMA
    sql += " = " + generateExpression(stmt.value);
  }

  return sql;
}

function generateColumnDefinition(col: ColumnDefinition): string {
  let sql = generateIdentifier(col.name) + " " + col.dataType;

  if (col.constraints && col.constraints.length > 0) {
    sql += " " + col.constraints.map(generateColumnConstraint).join(" ");
  }

  return sql;
}

function generateColumnConstraint(constraint: ColumnConstraint): string {
  if (constraint.constraint === "DEFAULT") {
    if (!constraint.value) {
      throw new Error("DEFAULT constraint requires a value");
    }
    return "DEFAULT " + generateExpression(constraint.value);
  }

  return constraint.constraint;
}

function generateExpression(expr: Expression): string {
  switch (expr.type) {
    case "Identifier":
      return generateIdentifier(expr);
    case "Literal":
      return generateLiteral(expr);
    case "Placeholder":
      return "?";
    case "BinaryExpression":
      return generateBinaryExpression(expr);
    case "UnaryExpression":
      return generateUnaryExpression(expr);
    case "FunctionCall":
      return generateFunctionCall(expr);
    case "InExpression":
      return generateInExpression(expr);
    case "BetweenExpression":
      return generateBetweenExpression(expr);
    case "SubqueryExpression":
      return generateSubqueryExpression(expr);
    case "CaseExpression":
      return generateCaseExpression(expr);
    default:
      throw new Error(`Unknown expression type: ${(expr as any).type}`);
  }
}

function generateIdentifier(id: Identifier): string {
  return id.name;
}

function generateLiteral(lit: Literal): string {
  return lit.raw;
}

function generateBinaryExpression(expr: BinaryExpression): string {
  const left = generateExpression(expr.left);
  const right = generateExpression(expr.right);
  return `${left} ${expr.operator} ${right}`;
}

function generateUnaryExpression(expr: UnaryExpression): string {
  const operand = generateExpression(expr.expression);

  // Handle IS NULL and IS NOT NULL specially
  if (expr.operator === "IS NULL" || expr.operator === "IS NOT NULL") {
    return `${operand} ${expr.operator}`;
  }

  return `${expr.operator} ${operand}`;
}

function generateFunctionCall(expr: FunctionCall): string {
  const args = expr.arguments.map(generateExpression).join(", ");
  return `${expr.name}(${args})`;
}

function generateInExpression(expr: InExpression): string {
  const value = generateExpression(expr.expression);
  const operator = expr.not ? "NOT IN" : "IN";

  // Check if this is a subquery
  if (expr.values.length === 1 && expr.values[0]!.type === "SubqueryExpression") {
    return `${value} ${operator} ${generateExpression(expr.values[0]!)}`;
  }

  const values = expr.values.map(generateExpression).join(", ");
  return `${value} ${operator} (${values})`;
}

function generateBetweenExpression(expr: BetweenExpression): string {
  const value = generateExpression(expr.expression);
  const operator = expr.not ? "NOT BETWEEN" : "BETWEEN";
  const lower = generateExpression(expr.lower);
  const upper = generateExpression(expr.upper);
  return `${value} ${operator} ${lower} AND ${upper}`;
}

function generateSubqueryExpression(expr: SubqueryExpression): string {
  return "(" + generateSelect(expr.query) + ")";
}

function generateCaseExpression(expr: CaseExpression): string {
  let sql = "CASE";

  if (expr.expression) {
    sql += " " + generateExpression(expr.expression);
  }

  for (const whenClause of expr.whenClauses) {
    sql += " WHEN " + generateExpression(whenClause.when);
    sql += " THEN " + generateExpression(whenClause.then);
  }

  if (expr.else) {
    sql += " ELSE " + generateExpression(expr.else);
  }

  sql += " END";

  return sql;
}
