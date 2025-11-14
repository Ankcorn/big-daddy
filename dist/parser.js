import { tokenize } from "./tokenizer";
export class ParserError extends Error {
    token;
    expected;
    constructor(message, token, expected) {
        super(message);
        this.token = token;
        this.expected = expected;
        this.name = "ParserError";
    }
}
class Parser {
    tokens;
    position = 0;
    placeholderCount = 0;
    constructor(tokens) {
        this.tokens = tokens;
    }
    current() {
        return this.tokens[this.position];
    }
    peek() {
        return this.tokens[this.position + 1];
    }
    advance() {
        return this.tokens[this.position++];
    }
    expect(tokenValue, type) {
        const token = this.current();
        if (!token) {
            throw new ParserError(`Expected '${tokenValue}' but reached end of input`);
        }
        if (token.token.toLowerCase() !== tokenValue.toLowerCase()) {
            throw new ParserError(`Expected '${tokenValue}' but got '${token.token}'`, token, tokenValue);
        }
        if (type && token.type !== type) {
            throw new ParserError(`Expected token type '${type}' but got '${token.type}'`, token, type);
        }
        return this.advance();
    }
    match(...values) {
        const token = this.current();
        if (!token)
            return false;
        return values.some(v => token.token.toLowerCase() === v.toLowerCase());
    }
    parse() {
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
        if (this.match("DROP")) {
            return this.parseDrop();
        }
        if (this.match("PRAGMA")) {
            return this.parsePragma();
        }
        throw new ParserError(`Unexpected statement starting with '${token.token}'`, token);
    }
    parseSelect() {
        this.expect("SELECT", "keyword");
        let distinct;
        if (this.match("DISTINCT")) {
            this.advance();
            distinct = true;
        }
        const select = this.parseSelectClauses();
        let from;
        if (this.match("FROM")) {
            this.advance();
            from = this.parseIdentifier();
        }
        let joins;
        if (this.match("JOIN", "LEFT", "RIGHT", "INNER", "OUTER")) {
            joins = this.parseJoins();
        }
        let where;
        if (this.match("WHERE")) {
            this.advance();
            where = this.parseExpression();
        }
        let groupBy;
        if (this.match("GROUP")) {
            this.advance();
            this.expect("BY", "keyword");
            groupBy = this.parseGroupBy();
        }
        let having;
        if (this.match("HAVING")) {
            this.advance();
            having = this.parseExpression();
        }
        let orderBy;
        if (this.match("ORDER")) {
            this.advance();
            this.expect("BY", "keyword");
            orderBy = this.parseOrderBy();
        }
        let limit;
        if (this.match("LIMIT")) {
            this.advance();
            limit = this.parsePrimary();
        }
        let offset;
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
    parseSelectClauses() {
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
            let alias;
            if (this.match("AS")) {
                this.advance();
                alias = this.parseIdentifier();
            }
            else if (this.current()?.type === "identifier" && !this.match("FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ORDER", "GROUP", "LIMIT", "HAVING")) {
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
    parseInsert() {
        this.expect("INSERT", "keyword");
        this.expect("INTO", "keyword");
        const table = this.parseIdentifier();
        let columns;
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
    parseUpdate() {
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
        let where;
        if (this.match("WHERE")) {
            this.advance();
            where = this.parseExpression();
        }
        let returning;
        if (this.match("RETURNING")) {
            this.advance();
            returning = this.parseSelectClauses();
        }
        // Skip trailing semicolon if present
        this.skipSemicolon();
        return {
            type: "UpdateStatement",
            table,
            set,
            ...(where && { where }),
            ...(returning && { returning })
        };
    }
    parseDelete() {
        this.expect("DELETE", "keyword");
        this.expect("FROM", "keyword");
        const table = this.parseIdentifier();
        let where;
        if (this.match("WHERE")) {
            this.advance();
            where = this.parseExpression();
        }
        let returning;
        if (this.match("RETURNING")) {
            this.advance();
            returning = this.parseSelectClauses();
        }
        // Skip trailing semicolon if present
        this.skipSemicolon();
        return {
            type: "DeleteStatement",
            table,
            ...(where && { where }),
            ...(returning && { returning })
        };
    }
    parseCreate() {
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
    parseCreateTable() {
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
    parseCreateIndex(unique) {
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
    parseAlter() {
        this.expect("ALTER", "keyword");
        if (this.match("TABLE")) {
            return this.parseAlterTable();
        }
        throw new ParserError("Only ALTER TABLE is supported");
    }
    parseAlterTable() {
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
        }
        else if (this.match("RENAME")) {
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
            }
            else if (this.match("COLUMN")) {
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
        }
        else if (this.match("DROP")) {
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
    parseDrop() {
        this.expect("DROP", "keyword");
        if (this.match("TABLE")) {
            return this.parseDropTable();
        }
        throw new ParserError("Only DROP TABLE is supported");
    }
    parseDropTable() {
        this.expect("TABLE", "keyword");
        // Check for IF EXISTS clause
        let ifExists = false;
        if (this.match("IF")) {
            this.advance();
            this.expect("EXISTS", "keyword");
            ifExists = true;
        }
        const table = this.parseIdentifier();
        return {
            type: "DropTableStatement",
            table,
            ifExists
        };
    }
    parsePragma() {
        this.expect("PRAGMA", "keyword");
        // Parse pragma name
        const nameToken = this.current();
        if (!nameToken || nameToken.type !== "identifier") {
            throw new ParserError("Expected pragma name", nameToken);
        }
        const name = this.advance().token;
        // Check if this is a function-call style PRAGMA (e.g., PRAGMA reshardTable(...))
        if (this.match("(")) {
            this.advance(); // consume (
            const args = this.match(")")
                ? []
                : this.parseCommaSeparatedList(() => this.parseExpression());
            this.expect(")", "punctuation");
            this.skipSemicolon();
            return {
                type: "PragmaStatement",
                name,
                arguments: args
            };
        }
        // Check if this is a key=value style PRAGMA (e.g., PRAGMA foreign_keys = ON)
        if (this.match("=")) {
            this.advance(); // consume =
            // In PRAGMA context, values can be keywords like ON, OFF, etc.
            // Try to parse as expression first, but if it fails, accept keywords as identifiers
            let value;
            const token = this.current();
            if (token && token.type === "keyword") {
                // Convert keyword to identifier for PRAGMA values
                const keywordValue = this.advance().token;
                value = { type: "Identifier", name: keywordValue };
            }
            else {
                value = this.parseExpression();
            }
            this.skipSemicolon();
            return {
                type: "PragmaStatement",
                name,
                value
            };
        }
        // Simple PRAGMA with no arguments or values (e.g., PRAGMA database_list)
        this.skipSemicolon();
        return {
            type: "PragmaStatement",
            name
        };
    }
    parseIdentifier() {
        const token = this.current();
        if (!token || token.type !== "identifier") {
            throw new ParserError(`Expected identifier but got '${token?.token}'`, token);
        }
        let name = this.advance().token;
        // Handle qualified identifiers (e.g., table.column)
        while (this.current()?.token === "." && this.current()?.type === "punctuation") {
            this.advance(); // consume the dot
            const nextToken = this.current();
            if (!nextToken || nextToken.type !== "identifier") {
                throw new ParserError(`Expected identifier after '.' but got '${nextToken?.token}'`, nextToken);
            }
            name += "." + this.advance().token;
        }
        return { type: "Identifier", name };
    }
    parseColumnConstraints(terminators = [",", ")"]) {
        const constraints = [];
        while (this.current() && !this.match(...terminators)) {
            if (this.match("PRIMARY")) {
                this.advance();
                this.expect("KEY", "keyword");
                constraints.push({
                    type: "ColumnConstraint",
                    constraint: "PRIMARY KEY"
                });
            }
            else if (this.match("NOT")) {
                this.advance();
                this.expect("NULL", "keyword");
                constraints.push({
                    type: "ColumnConstraint",
                    constraint: "NOT NULL"
                });
            }
            else if (this.match("NULL")) {
                this.advance();
                constraints.push({
                    type: "ColumnConstraint",
                    constraint: "NULL"
                });
            }
            else if (this.match("UNIQUE")) {
                this.advance();
                constraints.push({
                    type: "ColumnConstraint",
                    constraint: "UNIQUE"
                });
            }
            else if (this.match("AUTOINCREMENT")) {
                this.advance();
                constraints.push({
                    type: "ColumnConstraint",
                    constraint: "AUTOINCREMENT"
                });
            }
            else if (this.match("DEFAULT")) {
                this.advance();
                const value = this.parsePrimary();
                constraints.push({
                    type: "ColumnConstraint",
                    constraint: "DEFAULT",
                    value
                });
            }
            else {
                break;
            }
        }
        return constraints;
    }
    parseCommaSeparatedList(parseItem) {
        const items = [];
        do {
            if (this.match(",")) {
                this.advance();
            }
            items.push(parseItem());
        } while (this.match(","));
        return items;
    }
    skipSemicolon() {
        if (this.match(";")) {
            this.advance();
        }
    }
    parseIfNotExists() {
        if (this.match("IF")) {
            this.advance();
            this.expect("NOT", "keyword");
            this.expect("EXISTS", "keyword");
            return true;
        }
        return false;
    }
    parseDataType() {
        const dataTypeToken = this.current();
        if (!dataTypeToken || (dataTypeToken.type !== "identifier" && dataTypeToken.type !== "keyword")) {
            throw new ParserError(`Expected data type but got '${dataTypeToken?.token}'`, dataTypeToken);
        }
        return this.advance().token.toUpperCase();
    }
    parseColumnDefinition(terminators = [",", ")"]) {
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
    parseJoins() {
        const joins = [];
        while (this.match("JOIN", "LEFT", "RIGHT", "INNER", "OUTER")) {
            let joinType = "JOIN";
            // Handle LEFT/RIGHT/INNER/OUTER JOIN
            if (this.match("LEFT", "RIGHT", "INNER", "OUTER")) {
                const modifier = this.advance().token.toUpperCase();
                this.expect("JOIN", "keyword");
                joinType = `${modifier} JOIN`;
            }
            else {
                this.expect("JOIN", "keyword");
            }
            const table = this.parseIdentifier();
            let on;
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
    parseGroupBy() {
        return this.parseCommaSeparatedList(() => this.parseExpression());
    }
    parseOrderBy() {
        return this.parseCommaSeparatedList(() => {
            const expression = this.parseExpression();
            let direction;
            if (this.match("ASC", "DESC")) {
                direction = this.advance().token.toUpperCase();
            }
            return {
                type: "OrderByClause",
                expression,
                ...(direction && { direction })
            };
        });
    }
    parseExpression() {
        return this.parseLogicalOr();
    }
    parseLogicalOr() {
        let left = this.parseLogicalAnd();
        while (this.match("OR")) {
            const operator = this.advance().token;
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
    parseLogicalAnd() {
        let left = this.parseComparison();
        while (this.match("AND")) {
            const operator = this.advance().token;
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
    parseComparison() {
        const left = this.parsePrimary();
        // Check for IN operator
        if (this.match("IN")) {
            this.advance(); // consume IN
            this.expect("(", "punctuation");
            const values = [];
            // Check if this is a subquery
            if (this.match("SELECT")) {
                const query = this.parseSelect();
                values.push({
                    type: "SubqueryExpression",
                    query
                });
            }
            else if (!this.match(")")) {
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
            }
            else {
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
            const operator = this.advance().token;
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
    parsePrimary() {
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
            const name = this.advance().token;
            this.expect("(", "punctuation");
            // Handle empty function call or arguments
            const args = this.match(")") ? [] : this.parseCommaSeparatedList(() => {
                // Special case for * in COUNT(*)
                if (this.current()?.token === "*") {
                    this.advance();
                    return { type: "Identifier", name: "*" };
                }
                else {
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
            const raw = this.advance().token;
            const value = parseFloat(raw);
            return { type: "Literal", value, raw };
        }
        if (token.type === "string") {
            const raw = this.advance().token;
            // Remove quotes from string
            const value = raw.slice(1, -1);
            return { type: "Literal", value, raw };
        }
        if (token.type === "placeholder") {
            this.advance(); // consume ?
            return { type: "Placeholder", parameterIndex: this.placeholderCount++ };
        }
        throw new ParserError(`Unexpected token '${token.token}'`, token);
    }
    parseCaseExpression() {
        this.expect("CASE", "keyword");
        // Check if this is a simple CASE (CASE expression WHEN ...)
        // or searched CASE (CASE WHEN condition ...)
        let expression;
        if (!this.match("WHEN")) {
            expression = this.parseExpression();
        }
        const whenClauses = [];
        // Parse WHEN clauses
        while (this.match("WHEN")) {
            this.advance();
            const when = this.parseExpression();
            this.expect("THEN", "keyword");
            const then = this.parseExpression();
            whenClauses.push({ when, then });
        }
        // Parse optional ELSE clause
        let elseExpression;
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
export function parse(sql) {
    const tokens = tokenize(sql);
    const parser = new Parser(tokens);
    return parser.parse();
}
