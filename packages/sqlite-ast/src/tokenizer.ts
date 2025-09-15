export class TokenizerError extends Error {
  constructor(
    message: string,
    public readonly sql: string,
    public readonly position: number,
    public readonly character: string,
    public readonly tokenSnippet: string,
    public readonly errorType: 'unterminated_string' | 'unterminated_identifier' | 'invalid_token'
  ) {
    const detailedMessage = TokenizerError.createDetailedMessage(message, sql, position, character, tokenSnippet)
    super(detailedMessage)
    this.name = 'TokenizerError'
  }
  
  private static createDetailedMessage(
    message: string, 
    sql: string, 
    position: number, 
    character: string, 
    tokenSnippet: string
  ): string {
    const lines = sql.split('\n')
    let currentPos = 0
    let lineNumber = 1
    let columnNumber = 1
    
    // Find line and column of the error
    for (const line of lines) {
      if (currentPos + line.length >= position) {
        columnNumber = position - currentPos + 1
        break
      }
      currentPos += line.length + 1 // +1 for newline
      lineNumber++
    }
    
    const errorLine = lines[lineNumber - 1] || ''
    const pointer = ' '.repeat(Math.max(0, columnNumber - 1)) + '^'
    
    return `${message} at line ${lineNumber}, column ${columnNumber}
${errorLine}
${pointer}`
  }
}

function removeComments(sql: string): string {
  let result = ''
  let i = 0
  
  while (i < sql.length) {
    // Check for single-line comment
    if (i < sql.length - 1 && sql[i] === '-' && sql[i + 1] === '-') {
      // Skip until end of line
      while (i < sql.length && sql[i] !== '\n') {
        i++
      }
      if (i < sql.length) {
        result += '\n' // Keep the newline
        i++
      }
    }
    // Check for multi-line comment
    else if (i < sql.length - 1 && sql[i] === '/' && sql[i + 1] === '*') {
      i += 2 // Skip /*
      // Skip until */
      while (i < sql.length - 1) {
        if (sql[i] === '*' && sql[i + 1] === '/') {
          i += 2 // Skip */
          break
        }
        // Preserve newlines in multi-line comments
        if (sql[i] === '\n') {
          result += '\n'
        }
        i++
      }
    }
    else {
      result += sql[i]
      i++
    }
  }
  
  return result
}

enum QuoteState {
  NORMAL = 'normal',
  IN_SINGLE_QUOTE = 'single',
  IN_DOUBLE_QUOTE = 'double', 
  IN_BACKTICK = 'backtick'
}

const QUOTE_CONFIGS = {
  "'": { state: QuoteState.IN_SINGLE_QUOTE, allowBackslashEscape: true },
  '"': { state: QuoteState.IN_DOUBLE_QUOTE, allowBackslashEscape: false },
  '`': { state: QuoteState.IN_BACKTICK, allowBackslashEscape: false }
} as const

// Compile regex once at module load time for better performance
const TOKEN_REGEX = /(<=|>=|0x[0-9a-fA-F]+|0b[01]+|\d+\.?\d*[eE][+-]?\d+|\d+\.\d+|\w+|__LITERAL_\d+__|[(),;=<>*+.\/-]|\s+)/g

function extractQuotedLiterals(sql: string): { sql: string, literals: string[] } {
  const literals: string[] = []
  let result = ''
  let currentLiteral = ''
  let state = QuoteState.NORMAL
  let currentQuote = ''
  let quoteStartPosition = -1
  const originalSql = sql // Keep reference to original SQL for error reporting
  
  for (let i = 0; i < sql.length; i++) {
    const char = sql[i]
    const nextChar = sql[i + 1]
    
    switch (state) {
      case QuoteState.NORMAL:
        const quoteConfig = QUOTE_CONFIGS[char as keyof typeof QUOTE_CONFIGS]
        if (quoteConfig) {
          currentLiteral = char
          currentQuote = char
          quoteStartPosition = i
          state = quoteConfig.state
        } else {
          result += char
        }
        break
        
      case QuoteState.IN_SINGLE_QUOTE:
      case QuoteState.IN_DOUBLE_QUOTE:
      case QuoteState.IN_BACKTICK:
        currentLiteral += char
        
        if (char === currentQuote) {
          if (nextChar === currentQuote) {
            // SQL escape: handle '' or ""
            currentLiteral += currentQuote
            i++
          } else {
            // End of literal
            literals.push(currentLiteral)
            result += `__LITERAL_${literals.length - 1}__`
            currentLiteral = ''
            state = QuoteState.NORMAL
          }
        } else if (char === '\\' && nextChar && QUOTE_CONFIGS[currentQuote as keyof typeof QUOTE_CONFIGS]?.allowBackslashEscape) {
          // Backslash escape (only for single quotes)
          currentLiteral += nextChar
          i++
        }
        break
    }
  }
  
  // Check for unterminated literals
  if (state !== QuoteState.NORMAL) {
    const literalType = currentQuote === "'" ? 'string literal' : 'identifier'
    const errorType = currentQuote === "'" ? 'unterminated_string' : 'unterminated_identifier'
    
    throw new TokenizerError(
      `Unterminated ${literalType}`,
      originalSql,
      quoteStartPosition,
      currentQuote,
      currentLiteral.length > 50 ? currentLiteral.slice(0, 47) + '...' : currentLiteral,
      errorType
    )
  }
  
  return { sql: result, literals }
}

function splitTokens(sql: string): string[] {
  const tokens: string[] = []
  const originalSql = sql
  
  // Reset regex state since it's global
  TOKEN_REGEX.lastIndex = 0
  
  let match
  let lastIndex = 0
  
  while ((match = TOKEN_REGEX.exec(sql)) !== null) {
    const token = match[0].trim()
    if (token.length > 0) {
      // Calculate position of this token in original SQL
      const tokenPosition = sql.indexOf(match[0], lastIndex)
      
      tokens.push(token)
    }
    lastIndex = TOKEN_REGEX.lastIndex
  }
  
  return tokens
}

function restoreLiterals(tokens: string[], literals: string[]): string[] {
  return tokens.map(token => {
    const match = token.match(/__LITERAL_(\d+)__/)
    if (match) {
      const index = parseInt(match[1])
      return literals[index]
    }
    return token
  })
}

export function tokenize(sql: string): string[] {
  // Step 1: Remove comments
  const sqlWithoutComments = removeComments(sql)
  
  // Step 2: Extract quoted literals (strings and identifiers)
  const { sql: processedSql, literals } = extractQuotedLiterals(sqlWithoutComments)
  
  // Step 3: Split on delimiters and operators
  const tokens = splitTokens(processedSql)
  
  // Step 4: Restore quoted literals
  const finalTokens = restoreLiterals(tokens, literals)
  
  return finalTokens
}
