# Tokenizer Improvement Roadmap

### 2. Performance Optimizations

### 3. Better Error Handling
- [ ] **Invalid tokens** - Detect and report malformed SQL tokens
- [ ] **Position tracking** - Track line/column positions for better error messages

### 4. Token Metadata
- [ ] **Line/column positions** - Add position information for each token
- [ ] **Token types** - Classify tokens as keyword, identifier, literal, operator, etc.
- [ ] **Source mapping** - Map tokens back to original string positions for debugging/tooling

## Implementation Notes

### Performance Goals
- Current: O(n²) for multiple identical strings due to regex resets
- Target: O(n) single-pass tokenization
- Method: Character-by-character state machine instead of regex-based approach

### Error Handling Strategy
- Graceful degradation for malformed input
- Detailed error messages with context
- Continue tokenizing when possible vs. hard failures

### Metadata Structure
```typescript
interface Token {
  value: string
  type: 'keyword' | 'identifier' | 'literal' | 'operator' | 'delimiter'
  position: {
    start: { line: number, column: number }
    end: { line: number, column: number }
  }
  sourceIndex: { start: number, end: number }
}
```
