export class TokenizerError extends Error {
    sql;
    position;
    character;
    tokenSnippet;
    errorType;
    constructor(message, sql, position, character, tokenSnippet, errorType) {
        const detailedMessage = TokenizerError.createDetailedMessage(message, sql, position, character, tokenSnippet);
        super(detailedMessage);
        this.sql = sql;
        this.position = position;
        this.character = character;
        this.tokenSnippet = tokenSnippet;
        this.errorType = errorType;
        this.name = 'TokenizerError';
    }
    static createDetailedMessage(message, sql, position, character, tokenSnippet) {
        const lines = sql.split('\n');
        let currentPos = 0;
        let lineNumber = 1;
        let columnNumber = 1;
        // Find line and column of the error
        for (const line of lines) {
            if (currentPos + line.length >= position) {
                columnNumber = position - currentPos + 1;
                break;
            }
            currentPos += line.length + 1; // +1 for newline
            lineNumber++;
        }
        const errorLine = lines[lineNumber - 1] || '';
        const pointer = ' '.repeat(Math.max(0, columnNumber - 1)) + '^';
        return `${message} at line ${lineNumber}, column ${columnNumber}
${errorLine}
${pointer}`;
    }
}
function removeComments(sql) {
    let result = '';
    let i = 0;
    const positionMap = new Map();
    while (i < sql.length) {
        // Check for single-line comment
        if (i < sql.length - 1 && sql[i] === '-' && sql[i + 1] === '-') {
            // Skip until end of line
            while (i < sql.length && sql[i] !== '\n') {
                i++;
            }
            if (i < sql.length) {
                positionMap.set(result.length, i);
                result += '\n'; // Keep the newline
                i++;
            }
        }
        // Check for multi-line comment
        else if (i < sql.length - 1 && sql[i] === '/' && sql[i + 1] === '*') {
            i += 2; // Skip /*
            // Skip until */
            while (i < sql.length - 1) {
                if (sql[i] === '*' && sql[i + 1] === '/') {
                    i += 2; // Skip */
                    break;
                }
                // Preserve newlines in multi-line comments
                if (sql[i] === '\n') {
                    positionMap.set(result.length, i);
                    result += '\n';
                }
                i++;
            }
        }
        else {
            positionMap.set(result.length, i);
            result += sql[i];
            i++;
        }
    }
    return { sql: result, positionMap };
}
var QuoteState;
(function (QuoteState) {
    QuoteState["NORMAL"] = "normal";
    QuoteState["IN_SINGLE_QUOTE"] = "single";
    QuoteState["IN_DOUBLE_QUOTE"] = "double";
    QuoteState["IN_BACKTICK"] = "backtick";
})(QuoteState || (QuoteState = {}));
const QUOTE_CONFIGS = {
    "'": { state: QuoteState.IN_SINGLE_QUOTE, allowBackslashEscape: true },
    '"': { state: QuoteState.IN_DOUBLE_QUOTE, allowBackslashEscape: false },
    '`': { state: QuoteState.IN_BACKTICK, allowBackslashEscape: false }
};
// Compile regex once at module load time for better performance
const TOKEN_REGEX = /(<=|>=|!=|<>|0x[0-9a-fA-F]+|0b[01]+|\d+\.?\d*[eE][+-]?\d+|\d+\.\d+|\w+|__LITERAL_\d+__|[(),;=<>*+.\/-?]|\s+)/g;
function extractQuotedLiterals(sql, inputPositionMap) {
    const literals = [];
    const outputPositionMap = new Map();
    let result = '';
    let currentLiteral = '';
    let state = QuoteState.NORMAL;
    let currentQuote = '';
    let quoteStartPosition = -1;
    const originalSql = sql; // Keep reference to original SQL for error reporting
    for (let i = 0; i < sql.length; i++) {
        const char = sql[i];
        const nextChar = sql[i + 1];
        switch (state) {
            case QuoteState.NORMAL:
                const quoteConfig = QUOTE_CONFIGS[char];
                if (quoteConfig && char) {
                    currentLiteral = char;
                    currentQuote = char;
                    quoteStartPosition = inputPositionMap.get(i) ?? i;
                    state = quoteConfig.state;
                }
                else {
                    outputPositionMap.set(result.length, inputPositionMap.get(i) ?? i);
                    result += char;
                }
                break;
            case QuoteState.IN_SINGLE_QUOTE:
            case QuoteState.IN_DOUBLE_QUOTE:
            case QuoteState.IN_BACKTICK:
                currentLiteral += char;
                if (char === currentQuote) {
                    if (nextChar === currentQuote) {
                        // SQL escape: handle '' or ""
                        currentLiteral += currentQuote;
                        i++;
                    }
                    else {
                        // End of literal
                        // Single quotes are strings, double quotes and backticks are identifiers
                        const literalType = currentQuote === "'" ? 'string' : 'identifier';
                        const endPos = inputPositionMap.get(i) ?? i;
                        literals.push({
                            literal: currentLiteral,
                            start: quoteStartPosition,
                            end: endPos + 1,
                            type: literalType
                        });
                        const placeholder = `__LITERAL_${literals.length - 1}__`;
                        // Map each character of the placeholder to the original start position
                        for (let j = 0; j < placeholder.length; j++) {
                            outputPositionMap.set(result.length + j, quoteStartPosition);
                        }
                        result += placeholder;
                        currentLiteral = '';
                        state = QuoteState.NORMAL;
                    }
                }
                else if (char === '\\' && nextChar && QUOTE_CONFIGS[currentQuote]?.allowBackslashEscape) {
                    // Backslash escape (only for single quotes)
                    currentLiteral += nextChar;
                    i++;
                }
                break;
        }
    }
    // Check for unterminated literals
    if (state !== QuoteState.NORMAL) {
        const literalType = currentQuote === "'" ? 'string literal' : 'identifier';
        const errorType = currentQuote === "'" ? 'unterminated_string' : 'unterminated_identifier';
        throw new TokenizerError(`Unterminated ${literalType}`, originalSql, quoteStartPosition, currentQuote, currentLiteral.length > 50 ? currentLiteral.slice(0, 47) + '...' : currentLiteral, errorType);
    }
    return { sql: result, literals, positionMap: outputPositionMap };
}
// SQL Keywords for classification
const SQL_KEYWORDS = new Set([
    'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP',
    'TABLE', 'INDEX', 'VIEW', 'INTO', 'VALUES', 'SET', 'JOIN', 'LEFT', 'RIGHT', 'INNER',
    'OUTER', 'CROSS', 'ON', 'USING', 'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT', 'OFFSET',
    'AS', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'LIKE', 'BETWEEN', 'EXISTS', 'DISTINCT',
    'ALL', 'ANY', 'SOME', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IF', 'WITH', 'RECURSIVE',
    'UNION', 'INTERSECT', 'EXCEPT', 'ASC', 'DESC', 'NULLS', 'FIRST', 'LAST',
    'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'CHECK', 'UNIQUE', 'DEFAULT', 'AUTOINCREMENT',
    'CONSTRAINT', 'CASCADE', 'RESTRICT', 'NO', 'ACTION', 'DEFERRABLE', 'INITIALLY',
    'DEFERRED', 'IMMEDIATE', 'TEMPORARY', 'TEMP', 'VIRTUAL', 'GENERATED', 'STORED',
    'COLUMN', 'ADD', 'RENAME', 'TO', 'TRIGGER', 'BEFORE', 'AFTER', 'INSTEAD', 'OF',
    'FOR', 'EACH', 'ROW', 'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT', 'RELEASE',
    'TRANSACTION', 'PRAGMA', 'VACUUM', 'ANALYZE', 'ATTACH', 'DETACH', 'DATABASE',
    'EXPLAIN', 'QUERY', 'PLAN', 'CAST', 'COLLATE', 'GLOB', 'REGEXP', 'MATCH', 'ESCAPE',
    'ISNULL', 'NOTNULL', 'OVER', 'PARTITION', 'WINDOW', 'RANGE', 'ROWS', 'UNBOUNDED',
    'PRECEDING', 'FOLLOWING', 'CURRENT', 'EXCLUDE', 'TIES', 'OTHERS', 'GROUPS',
    // Special SQLite keywords
    'CURRENT_TIMESTAMP', 'CURRENT_DATE', 'CURRENT_TIME'
]);
// SQL Data Types - only treated as keywords in specific contexts
const SQL_DATA_TYPES = new Set([
    'INTEGER', 'INT', 'TINYINT', 'SMALLINT', 'MEDIUMINT', 'BIGINT',
    'TEXT', 'VARCHAR', 'CHAR', 'CHARACTER', 'NCHAR', 'NVARCHAR', 'CLOB',
    'REAL', 'DOUBLE', 'FLOAT', 'NUMERIC', 'DECIMAL', 'BOOLEAN', 'BOOL',
    'BLOB', 'TIMESTAMP', 'DATE', 'TIME', 'DATETIME',
]);
// Common SQL Functions
const SQL_FUNCTIONS = new Set([
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT', 'TOTAL',
    'ABS', 'ROUND', 'RANDOM', 'COALESCE', 'IFNULL', 'NULLIF', 'IIF',
    'LENGTH', 'SUBSTR', 'UPPER', 'LOWER', 'TRIM', 'LTRIM', 'RTRIM', 'REPLACE',
    'CHAR', 'HEX', 'QUOTE', 'SOUNDEX', 'UNICODE', 'INSTR', 'PRINTF',
    'DATE', 'TIME', 'DATETIME', 'JULIANDAY', 'STRFTIME', 'UNIXEPOCH',
    'JSON_EXTRACT', 'JSON_ARRAY', 'JSON_OBJECT', 'JSON_ARRAY_LENGTH', 'JSON_TYPE',
    'JSON_VALID', 'JSON_SET', 'JSON_INSERT', 'JSON_REPLACE', 'JSON_REMOVE', 'JSON_PATCH',
    'TYPEOF', 'LAST_INSERT_ROWID', 'CHANGES', 'TOTAL_CHANGES',
    'LIKELIHOOD', 'LIKELY', 'UNLIKELY', 'SQLITE_VERSION', 'SQLITE_SOURCE_ID',
    'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'PERCENT_RANK', 'CUME_DIST', 'NTILE',
    'LAG', 'LEAD', 'FIRST_VALUE', 'LAST_VALUE', 'NTH_VALUE',
    'SQRT', 'POWER', 'POW', 'EXP', 'LOG', 'LN', 'LOG10', 'LOG2',
    'SIN', 'COS', 'TAN', 'ASIN', 'ACOS', 'ATAN', 'ATAN2',
    'CEIL', 'CEILING', 'FLOOR', 'SIGN', 'DEGREES', 'RADIANS', 'PI',
    'CAST'
]);
function classifyToken(token, nextToken, prevToken, prevTokenType) {
    // Check if it's a placeholder
    if (token === '?') {
        return 'placeholder';
    }
    // Check if it's a number (integer, decimal, hex, binary, or scientific notation)
    if (/^-?\d+$/.test(token) ||
        /^\d+\.\d+$/.test(token) ||
        /^0x[0-9a-fA-F]+$/i.test(token) ||
        /^0b[01]+$/i.test(token) ||
        /^\d+\.?\d*[eE][+-]?\d+$/.test(token)) {
        return 'number';
    }
    // Check if it's an operator
    if (/^[+\-*\/<>=!]$/.test(token) || token === '<=' || token === '>=' || token === '!=' || token === '<>') {
        return 'operator';
    }
    // Check if it's punctuation
    if (/^[(),;.]$/.test(token)) {
        return 'punctuation';
    }
    const upperToken = token.toUpperCase();
    // Check if it's a function BEFORE checking keywords
    // This allows keywords like CAST, DATE, TIME, DATETIME to be recognized as functions when followed by (
    if (SQL_FUNCTIONS.has(upperToken) && nextToken === '(') {
        return 'function';
    }
    // Check if it's a data type in a valid context
    if (SQL_DATA_TYPES.has(upperToken)) {
        // Data types are keywords only after:
        // 1. An identifier in column definitions (prevTokenType === 'identifier')
        // 2. The AS keyword (in CAST expressions)
        if (prevTokenType === 'identifier' || (prevToken && prevToken.toUpperCase() === 'AS')) {
            return 'keyword';
        }
        // Otherwise, treat it as an identifier (e.g., column name)
        return 'identifier';
    }
    // Check if it's a keyword
    if (SQL_KEYWORDS.has(upperToken)) {
        return 'keyword';
    }
    // Default to identifier
    return 'identifier';
}
function splitTokens(sql, positionMap) {
    const tokens = [];
    // Reset regex state since it's global
    TOKEN_REGEX.lastIndex = 0;
    let match;
    while ((match = TOKEN_REGEX.exec(sql)) !== null) {
        const tokenStr = match[0].trim();
        if (tokenStr.length > 0) {
            // Find where the non-whitespace token starts within the match
            const wsPrefix = match[0].length - match[0].trimStart().length;
            const tokenStart = match.index + wsPrefix;
            const tokenEnd = tokenStart + tokenStr.length;
            // Map back to original SQL positions
            const originalStart = positionMap.get(tokenStart) ?? tokenStart;
            const originalEnd = positionMap.get(tokenEnd - 1) !== undefined
                ? (positionMap.get(tokenEnd - 1) ?? tokenEnd - 1) + 1
                : tokenEnd;
            tokens.push({
                token: tokenStr,
                start: originalStart,
                end: originalEnd
            });
        }
    }
    return tokens;
}
function restoreLiterals(tokens, literals) {
    const result = [];
    for (let i = 0; i < tokens.length; i++) {
        const token = tokens[i];
        if (!token)
            continue;
        const match = token.token.match(/__LITERAL_(\d+)__/);
        if (match && match[1]) {
            const index = parseInt(match[1]);
            const literalInfo = literals[index];
            if (literalInfo) {
                result.push({
                    token: literalInfo.literal,
                    type: literalInfo.type,
                    start: literalInfo.start,
                    end: literalInfo.end
                });
            }
        }
        else {
            // Classify non-literal tokens with context
            const nextToken = i < tokens.length - 1 ? tokens[i + 1]?.token : undefined;
            const prevToken = result.length > 0 ? result[result.length - 1]?.token : undefined;
            const prevTokenType = result.length > 0 ? result[result.length - 1]?.type : undefined;
            const type = classifyToken(token.token, nextToken, prevToken, prevTokenType);
            result.push({
                token: token.token,
                type,
                start: token.start,
                end: token.end
            });
        }
    }
    return result;
}
export function tokenize(sql) {
    // Step 1: Remove comments (and track position mapping)
    const { sql: sqlWithoutComments, positionMap: commentPositionMap } = removeComments(sql);
    // Step 2: Extract quoted literals (strings and identifiers) with position tracking
    const { sql: processedSql, literals, positionMap: literalPositionMap } = extractQuotedLiterals(sqlWithoutComments, commentPositionMap);
    // Step 3: Split on delimiters and operators (using position map)
    const tokens = splitTokens(processedSql, literalPositionMap);
    // Step 4: Restore quoted literals and classify tokens
    const finalTokens = restoreLiterals(tokens, literals);
    return finalTokens;
}
