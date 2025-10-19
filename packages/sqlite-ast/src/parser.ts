import { tokenize, type Token } from "./tokenizer";
import type {
  SelectStatement,
  InsertStatement,
  UpdateStatement,
  DeleteStatement,
  CreateTableStatement,
  AlterTableStatement,
  CreateIndexStatement,
  ColumnDefinition,
  ColumnConstraint,
  Statement,
  SelectClause,
  JoinClause,
  OrderByClause,
  FunctionCall,
  Identifier,
  Literal,
  BinaryExpression,
  UnaryExpression,
  InExpression,
  BetweenExpression,
  SubqueryExpression,
  CaseExpression,
  Expression
} from "./types";

export class ParserError extends Error {
  constructor(
    message: string,
    public readonly token?: Token,
    public readonly expected?: string
  ) {
    super(message);
    this.name = "ParserError";
  }
}

class Parser {
  private tokens: Token[];
  private position: number = 0;

  constructor(tokens: Token[]) {
    this.tokens = tokens;
  }

  private current(): Token | undefined {
    return this.tokens[this.position];
  }

  private peek(): Token | undefined {
    return this.tokens[this.position + 1];
  }

  private advance(): Token | undefined {
    return this.tokens[this.position++];
  }

  private expect(tokenValue: string, type?: string): Token {
    const token = this.current();
    if (!token) {
      throw new ParserError(`Expected '${tokenValue}' but reached end of input`);
    }
    if (token.token.toLowerCase() !== tokenValue.toLowerCase()) {
      throw new ParserError(
        `Expected '${tokenValue}' but got '${token.token}'`,
        token,
        tokenValue
      );
    }
    if (type && token.type !== type) {
      throw new ParserError(
        `Expected token type '${type}' but got '${token.type}'`,
        token,
        type
      );
    }
    return this.advance()!;
  }

  private match(...values: string[]): boolean {
    const token = this.current();
    if (!token) return false;
    return values.some(v => token.token.toLowerCase() === v.toLowerCase());
  }

  public parse(): Statement {
    const token = this.current();
    if (!token) {
      throw new ParserError("Empty input");
    }

    if (this.match("SELECT")) {
      return this.parseSelect();
    }

    if (this.match("INSERT")) {
      return this.parseInsert();
    }

    if (this.match("UPDATE")) {
      return this.parseUpdate();
    }

    if (this.match("DELETE")) {
      return this.parseDelete();
    }

    if (this.match("CREATE")) {
      return this.parseCreate();
    }

    if (this.match("ALTER")) {
      return this.parseAlter();
    }

    throw new ParserError(`Unexpected statement starting with '${token.token}'`, token);
  }

  private parseSelect(): SelectStatement {
    this.expect("SELECT", "keyword");

    let distinct: boolean | undefined;
    if (this.match("DISTINCT")) {
      this.advance();
      distinct = true;
    }

    const select = this.parseSelectClauses();

    let from: Identifier | undefined;
    if (this.match("FROM")) {
      this.advance();
      from = this.parseIdentifier();
    }

    let joins: JoinClause[] | undefined;
    if (this.match("JOIN", "LEFT", "RIGHT", "INNER", "OUTER")) {
      joins = this.parseJoins();
    }

    let where: Expression | undefined;
    if (this.match("WHERE")) {
      this.advance();
      where = this.parseExpression();
    }

    let groupBy: Expression[] | undefined;
    if (this.match("GROUP")) {
      this.advance();
      this.expect("BY", "keyword");
      groupBy = this.parseGroupBy();
    }

    let having: Expression | undefined;
    if (this.match("HAVING")) {
      this.advance();
      having = this.parseExpression();
    }

    let orderBy: OrderByClause[] | undefined;
    if (this.match("ORDER")) {
      this.advance();
      this.expect("BY", "keyword");
      orderBy = this.parseOrderBy();
    }

    let limit: Expression | undefined;
    if (this.match("LIMIT")) {
      this.advance();
      limit = this.parsePrimary();
    }

    let offset: Expression | undefined;
    if (this.match("OFFSET")) {
      this.advance();
      offset = this.parsePrimary();
    }

    // Skip trailing semicolon if present
    this.skipSemicolon();

    return {
      type: "SelectStatement",
      ...(distinct && { distinct }),
      select,
      from,
      joins,
      where,
      groupBy,
      having,
      orderBy,
      limit,
      offset
    };
  }

  private parseSelectClauses(): SelectClause[] {
    return this.parseCommaSeparatedList(() => {
      // Handle wildcard *
      if (this.current()?.token === "*" && this.current()?.type === "operator") {
        this.advance();
        return {
          type: "SelectClause",
          expression: { type: "Identifier", name: "*" }
        };
      }

      const expression = this.parseExpression();

      // Check for alias (AS keyword or implicit)
      let alias: Identifier | undefined;
      if (this.match("AS")) {
        this.advance();
        alias = this.parseIdentifier();
      } else if (this.current()?.type === "identifier" && !this.match("FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ORDER", "GROUP", "LIMIT", "HAVING")) {
        // Implicit alias (without AS keyword)
        alias = this.parseIdentifier();
      }

      return {
        type: "SelectClause",
        expression,
        ...(alias && { alias })
      };
    });
  }

  private parseInsert(): InsertStatement {
    this.expect("INSERT", "keyword");
    this.expect("INTO", "keyword");

    const table = this.parseIdentifier();

    let columns: Identifier[] | undefined;
    if (this.match("(")) {
      this.advance(); // consume (
      columns = this.parseCommaSeparatedList(() => this.parseIdentifier());
      this.expect(")", "punctuation");
    }

    this.expect("VALUES", "keyword");

    // Parse VALUES rows
    const values = this.parseCommaSeparatedList(() => {
      this.expect("(", "punctuation");
      const row = this.match(")") ? [] : this.parseCommaSeparatedList(() => this.parseExpression());
      this.expect(")", "punctuation");
      return row;
    });

    // Skip trailing semicolon if present
    this.skipSemicolon();

    return {
      type: "InsertStatement",
      table,
      ...(columns && { columns }),
      values
    };
  }

  private parseUpdate(): UpdateStatement {
    this.expect("UPDATE", "keyword");

    const table = this.parseIdentifier();

    this.expect("SET", "keyword");

    // Parse SET clauses
    const set = this.parseCommaSeparatedList(() => {
      const column = this.parseIdentifier();
      this.expect("=", "operator");
      const value = this.parseExpression();
      return { column, value };
    });

    let where: Expression | undefined;
    if (this.match("WHERE")) {
      this.advance();
      where = this.parseExpression();
    }

    // Skip trailing semicolon if present
    this.skipSemicolon();

    return {
      type: "UpdateStatement",
      table,
      set,
      ...(where && { where })
    };
  }

  private parseDelete(): DeleteStatement {
    this.expect("DELETE", "keyword");
    this.expect("FROM", "keyword");

    const table = this.parseIdentifier();

    let where: Expression | undefined;
    if (this.match("WHERE")) {
      this.advance();
      where = this.parseExpression();
    }

    // Skip trailing semicolon if present
    this.skipSemicolon();

    return {
      type: "DeleteStatement",
      table,
      ...(where && { where })
    };
  }

  private parseCreate(): Statement {
    this.expect("CREATE", "keyword");

    // Check for UNIQUE INDEX
    let unique = false;
    if (this.match("UNIQUE")) {
      this.advance();
      unique = true;
    }

    if (this.match("TABLE")) {
      return this.parseCreateTable();
    }

    if (this.match("INDEX")) {
      return this.parseCreateIndex(unique);
    }

    throw new ParserError("Only CREATE TABLE and CREATE INDEX are supported");
  }

  private parseCreateTable(): CreateTableStatement {
    this.expect("TABLE", "keyword");

    const ifNotExists = this.parseIfNotExists() || undefined;

    const table = this.parseIdentifier();

    this.expect("(", "punctuation");

    // Parse column definitions
    const columns = this.parseCommaSeparatedList(() => this.parseColumnDefinition());

    this.expect(")", "punctuation");

    // Skip trailing semicolon if present
    this.skipSemicolon();

    return {
      type: "CreateTableStatement",
      table,
      columns,
      ...(ifNotExists && { ifNotExists })
    };
  }

  private parseCreateIndex(unique: boolean): CreateIndexStatement {
    this.expect("INDEX", "keyword");

    const ifNotExists = this.parseIfNotExists() || undefined;

    const name = this.parseIdentifier();

    this.expect("ON", "keyword");

    const table = this.parseIdentifier();

    this.expect("(", "punctuation");

    // Parse column list
    const columns = this.parseCommaSeparatedList(() => this.parseIdentifier());

    this.expect(")", "punctuation");

    // Skip trailing semicolon if present
    this.skipSemicolon();

    return {
      type: "CreateIndexStatement",
      name,
      table,
      columns,
      ...(unique && { unique }),
      ...(ifNotExists && { ifNotExists })
    };
  }

  private parseAlter(): Statement {
    this.expect("ALTER", "keyword");

    if (this.match("TABLE")) {
      return this.parseAlterTable();
    }

    throw new ParserError("Only ALTER TABLE is supported");
  }

  private parseAlterTable(): AlterTableStatement {
    this.expect("TABLE", "keyword");

    const table = this.parseIdentifier();

    // Determine the action
    if (this.match("ADD")) {
      this.advance();
      this.expect("COLUMN", "keyword");

      const column = this.parseColumnDefinition([";"]);

      this.skipSemicolon();

      return {
        type: "AlterTableStatement",
        table,
        action: "ADD COLUMN",
        column
      };
    } else if (this.match("RENAME")) {
      this.advance();

      if (this.match("TO")) {
        this.advance();
        const newName = this.parseIdentifier();

        // Skip trailing semicolon if present
        if (this.match(";")) {
          this.advance();
        }

        return {
          type: "AlterTableStatement",
          table,
          action: "RENAME TO",
          newName
        };
      } else if (this.match("COLUMN")) {
        this.advance();
        const oldColumnName = this.parseIdentifier();
        this.expect("TO", "keyword");
        const newName = this.parseIdentifier();

        // Skip trailing semicolon if present
        if (this.match(";")) {
          this.advance();
        }

        return {
          type: "AlterTableStatement",
          table,
          action: "RENAME COLUMN",
          oldColumnName,
          newName
        };
      }

      throw new ParserError("Expected TO or COLUMN after RENAME");
    } else if (this.match("DROP")) {
      this.advance();
      this.expect("COLUMN", "keyword");
      const oldColumnName = this.parseIdentifier();

      // Skip trailing semicolon if present
      if (this.match(";")) {
        this.advance();
      }

      return {
        type: "AlterTableStatement",
        table,
        action: "DROP COLUMN",
        oldColumnName
      };
    }

    throw new ParserError("Expected ADD, RENAME, or DROP in ALTER TABLE");
  }

  private parseIdentifier(): Identifier {
    const token = this.current();
    if (!token || token.type !== "identifier") {
      throw new ParserError(
        `Expected identifier but got '${token?.token}'`,
        token
      );
    }

    let name = this.advance()!.token;

    // Handle qualified identifiers (e.g., table.column)
    while (this.current()?.token === "." && this.current()?.type === "punctuation") {
      this.advance(); // consume the dot
      const nextToken = this.current();
      if (!nextToken || nextToken.type !== "identifier") {
        throw new ParserError(
          `Expected identifier after '.' but got '${nextToken?.token}'`,
          nextToken
        );
      }
      name += "." + this.advance()!.token;
    }

    return { type: "Identifier", name };
  }

  private parseColumnConstraints(terminators: string[] = [",", ")"]): ColumnConstraint[] {
    const constraints: ColumnConstraint[] = [];
    while (this.current() && !this.match(...terminators)) {
      if (this.match("PRIMARY")) {
        this.advance();
        this.expect("KEY", "keyword");
        constraints.push({
          type: "ColumnConstraint",
          constraint: "PRIMARY KEY"
        });
      } else if (this.match("NOT")) {
        this.advance();
        this.expect("NULL", "keyword");
        constraints.push({
          type: "ColumnConstraint",
          constraint: "NOT NULL"
        });
      } else if (this.match("NULL")) {
        this.advance();
        constraints.push({
          type: "ColumnConstraint",
          constraint: "NULL"
        });
      } else if (this.match("UNIQUE")) {
        this.advance();
        constraints.push({
          type: "ColumnConstraint",
          constraint: "UNIQUE"
        });
      } else if (this.match("AUTOINCREMENT")) {
        this.advance();
        constraints.push({
          type: "ColumnConstraint",
          constraint: "AUTOINCREMENT"
        });
      } else if (this.match("DEFAULT")) {
        this.advance();
        const value = this.parsePrimary();
        constraints.push({
          type: "ColumnConstraint",
          constraint: "DEFAULT",
          value
        });
      } else {
        break;
      }
    }
    return constraints;
  }

  private parseCommaSeparatedList<T>(parseItem: () => T): T[] {
    const items: T[] = [];
    do {
      if (this.match(",")) {
        this.advance();
      }
      items.push(parseItem());
    } while (this.match(","));
    return items;
  }

  private skipSemicolon(): void {
    if (this.match(";")) {
      this.advance();
    }
  }

  private parseIfNotExists(): boolean {
    if (this.match("IF")) {
      this.advance();
      this.expect("NOT", "keyword");
      this.expect("EXISTS", "keyword");
      return true;
    }
    return false;
  }

  private parseDataType(): string {
    const dataTypeToken = this.current();
    if (!dataTypeToken || (dataTypeToken.type !== "identifier" && dataTypeToken.type !== "keyword")) {
      throw new ParserError(
        `Expected data type but got '${dataTypeToken?.token}'`,
        dataTypeToken
      );
    }
    return this.advance()!.token.toUpperCase();
  }

  private parseColumnDefinition(terminators: string[] = [",", ")"]): ColumnDefinition {
    const name = this.parseIdentifier();
    const dataType = this.parseDataType();
    const constraints = this.parseColumnConstraints(terminators);

    return {
      type: "ColumnDefinition",
      name,
      dataType,
      ...(constraints.length > 0 && { constraints })
    };
  }

  private parseJoins(): JoinClause[] {
    const joins: JoinClause[] = [];

    while (this.match("JOIN", "LEFT", "RIGHT", "INNER", "OUTER")) {
      let joinType: JoinClause["joinType"] = "JOIN";

      // Handle LEFT/RIGHT/INNER/OUTER JOIN
      if (this.match("LEFT", "RIGHT", "INNER", "OUTER")) {
        const modifier = this.advance()!.token.toUpperCase();
        this.expect("JOIN", "keyword");
        joinType = `${modifier} JOIN` as JoinClause["joinType"];
      } else {
        this.expect("JOIN", "keyword");
      }

      const table = this.parseIdentifier();

      let on: Expression | undefined;
      if (this.match("ON")) {
        this.advance();
        on = this.parseExpression();
      }

      joins.push({
        type: "JoinClause",
        joinType,
        table,
        on
      });
    }

    return joins;
  }

  private parseGroupBy(): Expression[] {
    return this.parseCommaSeparatedList(() => this.parseExpression());
  }

  private parseOrderBy(): OrderByClause[] {
    return this.parseCommaSeparatedList(() => {
      const expression = this.parseExpression();

      let direction: "ASC" | "DESC" | undefined;
      if (this.match("ASC", "DESC")) {
        direction = this.advance()!.token.toUpperCase() as "ASC" | "DESC";
      }

      return {
        type: "OrderByClause",
        expression,
        ...(direction && { direction })
      };
    });
  }

  private parseExpression(): Expression {
    return this.parseLogicalOr();
  }

  private parseLogicalOr(): Expression {
    let left = this.parseLogicalAnd();

    while (this.match("OR")) {
      const operator = this.advance()!.token;
      const right = this.parseLogicalAnd();
      left = {
        type: "BinaryExpression",
        operator,
        left,
        right
      };
    }

    return left;
  }

  private parseLogicalAnd(): Expression {
    let left = this.parseComparison();

    while (this.match("AND")) {
      const operator = this.advance()!.token;
      const right = this.parseComparison();
      left = {
        type: "BinaryExpression",
        operator,
        left,
        right
      };
    }

    return left;
  }

  private parseComparison(): Expression {
    const left = this.parsePrimary();

    // Check for IN operator
    if (this.match("IN")) {
      this.advance(); // consume IN
      this.expect("(", "punctuation");

      const values: Expression[] = [];

      // Check if this is a subquery
      if (this.match("SELECT")) {
        const query = this.parseSelect();
        values.push({
          type: "SubqueryExpression",
          query
        });
      } else if (!this.match(")")) {
        // Parse comma-separated values
        values.push(...this.parseCommaSeparatedList(() => this.parseExpression()));
      }

      this.expect(")", "punctuation");

      return {
        type: "InExpression",
        expression: left,
        values
      };
    }

    // Check for BETWEEN operator
    if (this.match("BETWEEN")) {
      this.advance(); // consume BETWEEN
      const lower = this.parsePrimary();
      this.expect("AND", "keyword");
      const upper = this.parsePrimary();

      return {
        type: "BetweenExpression",
        expression: left,
        lower,
        upper
      };
    }

    // Check for IS NULL / IS NOT NULL
    if (this.match("IS")) {
      this.advance(); // consume IS
      let operator = "IS NULL";

      if (this.match("NOT")) {
        this.advance(); // consume NOT
        this.expect("NULL", "keyword");
        operator = "IS NOT NULL";
      } else {
        this.expect("NULL", "keyword");
      }

      return {
        type: "UnaryExpression",
        operator,
        expression: left
      };
    }

    // Check if this is a comparison expression
    const current = this.current();
    if (current && (current.type === "operator" || this.match("LIKE"))) {
      const operator = this.advance()!.token;
      const right = this.parsePrimary();

      return {
        type: "BinaryExpression",
        operator,
        left,
        right
      };
    }

    return left;
  }

  private parsePrimary(): Expression {
    const token = this.current();
    if (!token) {
      throw new ParserError("Unexpected end of input");
    }

    // Handle CASE expression
    if (this.match("CASE")) {
      return this.parseCaseExpression();
    }

    // Handle parenthesized expressions or subqueries
    if (token.type === "punctuation" && token.token === "(") {
      this.advance(); // consume (

      // Check if this is a subquery
      if (this.match("SELECT")) {
        const query = this.parseSelect();
        this.expect(")", "punctuation");
        return {
          type: "SubqueryExpression",
          query
        };
      }

      // Otherwise it's a parenthesized expression
      const expression = this.parseExpression();
      this.expect(")", "punctuation");
      return expression;
    }

    // Check for function call
    if (token.type === "function") {
      const name = this.advance()!.token;
      this.expect("(", "punctuation");

      // Handle empty function call or arguments
      const args = this.match(")") ? [] : this.parseCommaSeparatedList((): Expression => {
        // Special case for * in COUNT(*)
        if (this.current()?.token === "*") {
          this.advance();
          return { type: "Identifier", name: "*" } as Identifier;
        } else {
          return this.parseExpression();
        }
      });

      this.expect(")", "punctuation");

      return {
        type: "FunctionCall",
        name,
        arguments: args
      };
    }

    if (token.type === "identifier") {
      return this.parseIdentifier();
    }

    if (token.type === "number") {
      const raw = this.advance()!.token;
      const value = parseFloat(raw);
      return { type: "Literal", value, raw };
    }

    if (token.type === "string") {
      const raw = this.advance()!.token;
      // Remove quotes from string
      const value = raw.slice(1, -1);
      return { type: "Literal", value, raw };
    }

    if (token.type === "placeholder") {
      this.advance(); // consume ?
      return { type: "Placeholder" };
    }

    throw new ParserError(`Unexpected token '${token.token}'`, token);
  }

  private parseCaseExpression(): CaseExpression {
    this.expect("CASE", "keyword");

    // Check if this is a simple CASE (CASE expression WHEN ...)
    // or searched CASE (CASE WHEN condition ...)
    let expression: Expression | undefined;
    if (!this.match("WHEN")) {
      expression = this.parseExpression();
    }

    const whenClauses: { when: Expression; then: Expression }[] = [];

    // Parse WHEN clauses
    while (this.match("WHEN")) {
      this.advance();
      const when = this.parseExpression();
      this.expect("THEN", "keyword");
      const then = this.parseExpression();
      whenClauses.push({ when, then });
    }

    // Parse optional ELSE clause
    let elseExpression: Expression | undefined;
    if (this.match("ELSE")) {
      this.advance();
      elseExpression = this.parseExpression();
    }

    this.expect("END", "keyword");

    return {
      type: "CaseExpression",
      ...(expression && { expression }),
      whenClauses,
      ...(elseExpression && { else: elseExpression })
    };
  }
}

export function parse(sql: string): Statement {
  const tokens = tokenize(sql);
  const parser = new Parser(tokens);
  return parser.parse();
}
