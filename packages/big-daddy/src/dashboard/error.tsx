/**
 * Error page component
 */
export const ErrorPage = ({
	databaseId,
	errorMessage,
}: {
	databaseId: string;
	errorMessage: string;
}) => (
	<>
		<title>Big Daddy Dashboard - Error</title>
		<div class="max-w-2xl mx-auto mt-12 bg-white border-2 border-black p-8">
			<h1 class="text-2xl font-bold text-black mb-4">
				Error Loading Dashboard
			</h1>
			<p class="text-black mb-6">
				Failed to load topology information for database:{" "}
				<span class="font-mono text-sm">{databaseId}</span>
			</p>
			<div class="bg-white border-2 border-black text-black p-4 font-mono text-xs break-words max-h-96 overflow-y-auto leading-relaxed">
				{errorMessage}
			</div>
		</div>
	</>
);
