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

export interface BinaryExpression extends BaseNode {
  type: 'BinaryExpression'
  left: Expression
  operator: string
  right: Expression
}

export type Expression = 
  | Identifier
  | Literal
  | BinaryExpression

export interface SelectStatement extends BaseNode {
  type: 'SelectStatement'
  select: SelectClause[]
  from?: Identifier
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

export type Statement = 
  | SelectStatement
  | InsertStatement

export interface Program extends BaseNode {
  type: 'Program'
  body: Statement[]
}