/**
 * Artillery.io processor for Big Daddy benchmarks
 *
 * Adds correlation IDs to requests and captures them from responses
 * for distributed tracing and debugging.
 */

module.exports = {
	// Before each request, add a correlation ID header
	beforeRequest: (requestParams, context, ee, next) => {
		const correlationId = `artillery-${Date.now()}-${Math.random().toString(36).substring(7)}`;

		// Add correlation ID to request headers
		requestParams.headers = requestParams.headers || {};
		requestParams.headers['x-correlation-id'] = correlationId;

		// Store in context for later use
		context.vars.correlationId = correlationId;

		return next();
	},

	// After each response, capture the correlation ID for verification
	captureCorrelationId: (requestParams, response, context, ee, next) => {
		try {
			const body = JSON.parse(response.body);
			if (body.correlationId && body.correlationId !== context.vars.correlationId) {
				console.warn('Correlation ID mismatch!', {
					sent: context.vars.correlationId,
					received: body.correlationId
				});
			}

			// Track response times by operation type
			if (body.duration) {
				ee.emit('customStat', {
					stat: `response_time_${requestParams.url.split('/')[1] || 'root'}`,
					value: body.duration
				});
			}

			// Track cache performance
			if (body.cacheStats) {
				ee.emit('counter', `cache_hits`, body.cacheStats.cacheHit ? 1 : 0);
				ee.emit('counter', `cache_misses`, body.cacheStats.cacheHit ? 0 : 1);
			}
		} catch (error) {
			// Ignore parsing errors for non-JSON responses
		}

		return next();
	}
};
