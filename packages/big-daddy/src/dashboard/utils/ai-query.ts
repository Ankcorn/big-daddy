import { parse } from '@databases/sqlite-ast';
import { logger } from '../../logger';

/**
 * Try to parse a query using sqlite-ast, and if it fails, use Workers AI to convert it
 *
 * @param query - The user's natural language or SQL query
 * @param ai - The Workers AI binding
 * @returns A valid SQL query string
 */
export async function parseQueryWithAI(query: string, ai: Ai): Promise<string> {
	// First, try to parse the query with sqlite-ast
	try {
		const statement = parse(query);
		logger.debug('Query parsed successfully with sqlite-ast', {
			queryType: statement.type,
		});
		return query; // It's already valid SQL
	} catch (parseError) {
		logger.debug('Query parsing failed, attempting to use AI', {
			error: parseError instanceof Error ? parseError.message : String(parseError),
		});

		// The sqlite-ast parse failed, so try to use Workers AI to convert the query
		try {
			const messages = [
				{
					role: 'system' as const,
					content:
						'The user has requested to run something in our sqlite database. You need to turn their prompt into a valid sqlite query doing what the user has instructed. ' +
						'Guidelines: ' +
						'1. For bulk inserts (e.g., "insert 100 rows"), generate explicit VALUES clauses like INSERT INTO table (col) VALUES (\'val1\'), (\'val2\'), ... (\'valN\'); ' +
						'2. Do not use SELECT ... LIMIT or assume data already exists in the table. ' +
						'3. Always generate syntactically valid SQLite that can execute immediately. ' +
						'4. Use simple, direct approaches over complex subqueries when possible. ' +
						'5. Return ONLY the SQL query, no explanations.',
				},
				{
					role: 'user' as const,
					content: query,
				},
			];

			logger.debug('Calling Workers AI to generate SQL', {
				model: '@cf/qwen/qwen2.5-coder-32b-instruct',
			});

			const response = await ai.run('@cf/qwen/qwen2.5-coder-32b-instruct', {
				messages,
				max_tokens: 1024,
			});

			// Extract the text response from the AI model
			let generatedQuery: string;
			if (typeof response === 'string') {
				generatedQuery = response;
			} else if (response && typeof response === 'object') {
				// Handle various response formats from Workers AI
				const resp = response as any;
				generatedQuery = resp.response || resp.result?.response || resp.text || JSON.stringify(resp);
			} else {
				generatedQuery = String(response);
			}

			// Clean up the query - extract SQL if it's wrapped in a SELECT statement or quotes
			generatedQuery = generatedQuery.trim();

			// If the response is a SELECT statement wrapping the SQL, extract it
			const selectMatch = generatedQuery.match(/SELECT\s+['"`](.+)['"`]\s+AS\s+\w+/is);
			if (selectMatch && selectMatch[1]) {
				generatedQuery = selectMatch[1];
			}

			// Remove any surrounding quotes
			generatedQuery = generatedQuery.replace(/^['"`]+|['"`]+$/g, '');

			logger.debug('AI generated SQL query', {
				generatedQuery,
			});

			// Verify the generated query is valid by parsing it
			try {
				parse(generatedQuery);
				logger.debug('Generated query parsed successfully');
				return generatedQuery;
			} catch (generatedParseError) {
				logger.error('Generated query failed to parse', {
					generatedQuery,
					error: generatedParseError instanceof Error ? generatedParseError.message : String(generatedParseError),
				});
				throw new Error(`AI generated an invalid SQL query: ${generatedQuery}`);
			}
		} catch (aiError) {
			logger.error('Failed to generate SQL query with AI', {
				error: aiError instanceof Error ? aiError.message : String(aiError),
			});
			throw new Error(`Could not parse query and AI generation failed: ${aiError instanceof Error ? aiError.message : String(aiError)}`);
		}
	}
}
