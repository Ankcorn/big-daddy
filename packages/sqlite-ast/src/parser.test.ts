import { describe, it, expect } from 'vitest'
import { parse } from './parser'

describe('parser', () => {
  describe('basic SELECT statements', () => {
    it('should parse simple SELECT *', () => {
      const query = 'SELECT * FROM users;'
      const ast = parse(query)
      
      expect(ast).toEqual({
        type: 'Program',
        body: [{
          type: 'SelectStatement',
          select: [{
            type: 'SelectClause',
            expression: {
              type: 'Identifier',
              name: '*'
            }
          }],
          from: {
            type: 'Identifier',
            name: 'users'
          }
        }]
      })
    })

    it('should parse SELECT with specific columns', () => {
      const query = 'SELECT name, email FROM users;'
      const ast = parse(query)
      
      expect(ast).toEqual({
        type: 'Program',
        body: [{
          type: 'SelectStatement',
          select: [
            {
              type: 'SelectClause',
              expression: {
                type: 'Identifier',
                name: 'name'
              }
            },
            {
              type: 'SelectClause', 
              expression: {
                type: 'Identifier',
                name: 'email'
              }
            }
          ],
          from: {
            type: 'Identifier',
            name: 'users'
          }
        }]
      })
    })
  })

  describe('basic INSERT statements', () => {
    it('should parse simple INSERT with VALUES', () => {
      const query = "INSERT INTO users VALUES ('John', 'john@example.com');"
      const ast = parse(query)
      
      expect(ast).toEqual({
        type: 'Program',
        body: [{
          type: 'InsertStatement',
          table: {
            type: 'Identifier',
            name: 'users'
          },
          values: [[
            {
              type: 'Literal',
              value: 'John',
              raw: "'John'"
            },
            {
              type: 'Literal',
              value: 'john@example.com',
              raw: "'john@example.com'"
            }
          ]]
        }]
      })
    })

    it('should parse INSERT with column list', () => {
      const query = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com');"
      const ast = parse(query)
      
      expect(ast).toEqual({
        type: 'Program',
        body: [{
          type: 'InsertStatement',
          table: {
            type: 'Identifier',
            name: 'users'
          },
          columns: [
            {
              type: 'Identifier',
              name: 'name'
            },
            {
              type: 'Identifier',
              name: 'email'
            }
          ],
          values: [[
            {
              type: 'Literal',
              value: 'John',
              raw: "'John'"
            },
            {
              type: 'Literal',
              value: 'john@example.com',
              raw: "'john@example.com'"
            }
          ]]
        }]
      })
    })
  })
})