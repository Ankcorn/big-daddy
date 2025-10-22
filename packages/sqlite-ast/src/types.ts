export interface BaseNode {
  type: string
}

export interface Identifier extends BaseNode {
  type: 'Identifier'
  name: string
}

export interface Literal extends BaseNode {
  type: 'Literal'
  value: string | number | null | boolean
  raw: string
}

export interface Placeholder extends BaseNode {
  type: 'Placeholder'
  parameterIndex: number // Zero-based index of this placeholder in the query
}

export interface BinaryExpression extends BaseNode {
  type: 'BinaryExpression'
  left: Expression
  operator: string
  right: Expression
}

export interface UnaryExpression extends BaseNode {
  type: 'UnaryExpression'
  operator: string
  expression: Expression
}

export interface FunctionCall extends BaseNode {
  type: 'FunctionCall'
  name: string
  arguments: Expression[]
}

export interface InExpression extends BaseNode {
  type: 'InExpression'
  expression: Expression
  values: Expression[]
  not?: boolean
}

export interface BetweenExpression extends BaseNode {
  type: 'BetweenExpression'
  expression: Expression
  lower: Expression
  upper: Expression
  not?: boolean
}

export interface SubqueryExpression extends BaseNode {
  type: 'SubqueryExpression'
  query: SelectStatement
}

export interface CaseExpression extends BaseNode {
  type: 'CaseExpression'
  expression?: Expression
  whenClauses: { when: Expression; then: Expression }[]
  else?: Expression
}

export type Expression =
  | Identifier
  | Literal
  | Placeholder
  | BinaryExpression
  | UnaryExpression
  | FunctionCall
  | InExpression
  | BetweenExpression
  | SubqueryExpression
  | CaseExpression

export interface JoinClause extends BaseNode {
  type: 'JoinClause'
  joinType: 'JOIN' | 'LEFT JOIN' | 'RIGHT JOIN' | 'INNER JOIN' | 'OUTER JOIN'
  table: Identifier
  on?: Expression
}

export interface OrderByClause extends BaseNode {
  type: 'OrderByClause'
  expression: Expression
  direction?: 'ASC' | 'DESC'
}

export interface SelectStatement extends BaseNode {
  type: 'SelectStatement'
  distinct?: boolean
  select: SelectClause[]
  from?: Identifier
  joins?: JoinClause[]
  where?: Expression
  groupBy?: Expression[]
  having?: Expression
  orderBy?: OrderByClause[]
  limit?: Expression
  offset?: Expression
}

export interface SelectClause extends BaseNode {
  type: 'SelectClause'
  expression: Expression
  alias?: Identifier
}

export interface InsertStatement extends BaseNode {
  type: 'InsertStatement'
  table: Identifier
  columns?: Identifier[]
  values: Expression[][]
}

export interface UpdateStatement extends BaseNode {
  type: 'UpdateStatement'
  table: Identifier
  set: { column: Identifier; value: Expression }[]
  where?: Expression
}

export interface DeleteStatement extends BaseNode {
  type: 'DeleteStatement'
  table: Identifier
  where?: Expression
}

export interface ColumnDefinition extends BaseNode {
  type: 'ColumnDefinition'
  name: Identifier
  dataType: string
  constraints?: ColumnConstraint[]
}

export interface ColumnConstraint extends BaseNode {
  type: 'ColumnConstraint'
  constraint: 'PRIMARY KEY' | 'NOT NULL' | 'NULL' | 'UNIQUE' | 'AUTOINCREMENT' | 'DEFAULT'
  value?: Expression
}

export interface CreateTableStatement extends BaseNode {
  type: 'CreateTableStatement'
  table: Identifier
  columns: ColumnDefinition[]
  ifNotExists?: boolean
}

export interface AlterTableStatement extends BaseNode {
  type: 'AlterTableStatement'
  table: Identifier
  action: 'ADD COLUMN' | 'RENAME TO' | 'RENAME COLUMN' | 'DROP COLUMN'
  column?: ColumnDefinition
  newName?: Identifier
  oldColumnName?: Identifier
}

export interface CreateIndexStatement extends BaseNode {
  type: 'CreateIndexStatement'
  name: Identifier
  table: Identifier
  columns: Identifier[]
  unique?: boolean
  ifNotExists?: boolean
}

export type Statement =
  | SelectStatement
  | InsertStatement
  | UpdateStatement
  | DeleteStatement
  | CreateTableStatement
  | AlterTableStatement
  | CreateIndexStatement

export interface Program extends BaseNode {
  type: 'Program'
  body: Statement[]
}